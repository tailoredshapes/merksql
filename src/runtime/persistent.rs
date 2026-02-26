use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use anyhow::Result;
use merkql::broker::{Broker, BrokerRef};
use merkql::consumer::{ConsumerConfig, OffsetReset};
use merkql::record::ProducerRecord;

use crate::engine::operator::{DeserializeOp, Operator};
use crate::engine::pipeline;
use crate::plan::QueryPlan;
use crate::schema::SchemaRegistry;
use crate::types::{Row, RowMetadata, Schema, Value};

/// Status of a persistent query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryStatus {
    Running,
    Stopped,
    Terminated,
    Error,
}

/// A persistent query running as a background thread.
pub struct PersistentQuery {
    pub id: String,
    pub plan: QueryPlan,
    pub sink_topic: String,
    running: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
    status: Arc<std::sync::Mutex<QueryStatus>>,
}

impl PersistentQuery {
    /// Start a persistent query as a background thread.
    pub fn start(
        id: String,
        plan: QueryPlan,
        sink_topic: String,
        broker: BrokerRef,
        registry: &SchemaRegistry,
    ) -> Result<Self> {
        let running = Arc::new(AtomicBool::new(true));
        let status = Arc::new(std::sync::Mutex::new(QueryStatus::Running));

        let mut compiled = pipeline::compile(&plan, registry)?;
        let output_schema = compiled.output_schema.clone();

        // For stream-table joins: load right side before entering the loop,
        // and only subscribe to left-side topics for the continuous loop.
        let left_topics: Vec<String>;
        let right_topic = compiled.right_source_topic.clone();
        let right_schema = compiled.right_schema.clone();

        if right_topic.is_some() {
            // For joins, only loop on left-side (first) source topics
            let source_names = plan.source_names();
            left_topics = source_names
                .into_iter()
                .take(1)
                .filter_map(|name| registry.get(&name).map(|info| info.topic.clone()))
                .collect();
        } else {
            left_topics = compiled.source_topics.clone();
        }

        let running_clone = running.clone();
        let status_clone = status.clone();
        let broker_clone = broker.clone();
        let sink_topic_clone = sink_topic.clone();
        let group_id = format!("_merksql_query_{}", id);

        let handle = thread::spawn(move || {
            // If this is a join, load right-side data once at startup
            if let (Some(rt), Some(rs)) = (right_topic, right_schema) {
                if let Err(_) = load_right_side(&broker_clone, &mut compiled, &rt, &rs) {
                    *status_clone.lock().unwrap() = QueryStatus::Error;
                    return;
                }
            }

            let result = run_query_loop(
                &broker_clone,
                &mut compiled,
                &left_topics,
                &sink_topic_clone,
                &group_id,
                &running_clone,
                &output_schema,
            );

            let mut s = status_clone.lock().unwrap();
            if *s == QueryStatus::Running {
                match result {
                    Ok(()) => *s = QueryStatus::Stopped,
                    Err(_) => *s = QueryStatus::Error,
                }
            }
        });

        Ok(Self {
            id,
            plan,
            sink_topic,
            running,
            handle: Some(handle),
            status,
        })
    }

    /// Get the current status.
    pub fn status(&self) -> QueryStatus {
        *self.status.lock().unwrap()
    }

    /// Stop the query gracefully (commits offsets).
    pub fn stop(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        let mut s = self.status.lock().unwrap();
        if *s == QueryStatus::Running {
            *s = QueryStatus::Stopped;
        }
    }

    /// Terminate the query immediately.
    pub fn terminate(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        *self.status.lock().unwrap() = QueryStatus::Terminated;
    }
}

/// Load right-side table data into the join operator at startup.
fn load_right_side(
    broker: &BrokerRef,
    pipeline: &mut pipeline::Pipeline,
    right_topic: &str,
    right_schema: &Schema,
) -> Result<()> {
    let mut consumer = Broker::consumer(
        broker,
        ConsumerConfig {
            group_id: format!("_merksql_join_right_{}", uuid::Uuid::new_v4()),
            auto_commit: false,
            offset_reset: OffsetReset::Earliest,
        },
    );
    consumer.subscribe(&[right_topic])?;
    let records = consumer.poll(Duration::from_millis(100))?;
    consumer.close()?;

    if !records.is_empty() {
        let raw_rows: Vec<Row> = records
            .into_iter()
            .map(|r| {
                Row::with_metadata(
                    vec![Value::String(r.value)],
                    RowMetadata {
                        topic: Some(r.topic),
                        partition: Some(r.partition),
                        offset: Some(r.offset),
                        timestamp: Some(r.timestamp),
                        key: r.key,
                        ..Default::default()
                    },
                )
            })
            .collect();

        let mut deser = DeserializeOp::new(right_schema.clone());
        let right_rows = deser.process(raw_rows)?;
        pipeline.load_join_right(right_rows);
    }

    Ok(())
}

fn run_query_loop(
    broker: &BrokerRef,
    pipeline: &mut pipeline::Pipeline,
    source_topics: &[String],
    sink_topic: &str,
    group_id: &str,
    running: &AtomicBool,
    output_schema: &Schema,
) -> Result<()> {
    let topic_refs: Vec<&str> = source_topics.iter().map(|s| s.as_str()).collect();

    let mut consumer = Broker::consumer(
        broker,
        ConsumerConfig {
            group_id: group_id.to_string(),
            auto_commit: false,
            offset_reset: OffsetReset::Earliest,
        },
    );
    consumer.subscribe(&topic_refs)?;

    let producer = Broker::producer(broker);

    // Ensure sink topic exists
    broker.ensure_topic(sink_topic)?;

    while running.load(Ordering::SeqCst) {
        let records = consumer.poll(Duration::from_millis(100))?;

        if records.is_empty() {
            // Sleep briefly to avoid busy-waiting
            thread::sleep(Duration::from_millis(50));
            continue;
        }

        // Convert records to rows
        let rows: Vec<Row> = records
            .into_iter()
            .map(|r| {
                Row::with_metadata(
                    vec![Value::String(r.value)],
                    RowMetadata {
                        topic: Some(r.topic),
                        partition: Some(r.partition),
                        offset: Some(r.offset),
                        timestamp: Some(r.timestamp),
                        key: r.key,
                        ..Default::default()
                    },
                )
            })
            .collect();

        // Process through pipeline
        let output = pipeline.process(rows)?;

        // Produce output rows to sink topic
        for row in &output {
            let json = row_to_json(row, output_schema);
            let key = row.metadata.key.clone();
            producer.send(&ProducerRecord::new(sink_topic, key, json))?;
        }

        // Commit offsets
        consumer.commit_sync()?;
    }

    consumer.close()?;
    Ok(())
}

fn row_to_json(row: &Row, schema: &Schema) -> String {
    let mut map = serde_json::Map::new();
    for (i, col) in schema.columns.iter().enumerate() {
        let val = row
            .values
            .get(i)
            .map(serde_json::Value::from)
            .unwrap_or(serde_json::Value::Null);
        map.insert(col.name.clone(), val);
    }
    serde_json::Value::Object(map).to_string()
}
