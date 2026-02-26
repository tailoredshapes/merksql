use std::time::Duration;

use anyhow::Result;

use merkql::broker::BrokerRef;
use merkql::consumer::{ConsumerConfig, OffsetReset};

use crate::engine::operator::DeserializeOp;
use crate::engine::pipeline;
use crate::plan::QueryPlan;
use crate::schema::SchemaRegistry;
use crate::types::{Row, RowMetadata, Value};

/// Execute a pull query: read all records from source topics, process through
/// the pipeline, and return results as a point-in-time snapshot.
pub fn pull_query(
    broker: &BrokerRef,
    plan: &QueryPlan,
    registry: &SchemaRegistry,
) -> Result<Vec<Row>> {
    let mut pipeline = pipeline::compile(plan, registry)?;

    // If this is a join query, load the right side first
    if let (Some(right_topic), Some(right_schema)) = (
        pipeline.right_source_topic.clone(),
        pipeline.right_schema.clone(),
    ) {
        let right_raw = consume_all(broker, &[right_topic])?;
        if !right_raw.is_empty() {
            // Deserialize right-side rows using the right schema
            let mut deser = DeserializeOp::new(right_schema);
            let right_rows = crate::engine::operator::Operator::process(&mut deser, right_raw)?;
            pipeline.load_join_right(right_rows);
        }

        // Consume only left-side topics (first source) for processing
        let left_topics: Vec<String> = plan
            .source_names()
            .into_iter()
            .take(1)
            .filter_map(|name| registry.get(&name).map(|info| info.topic.clone()))
            .collect();
        let left_rows = consume_all(broker, &left_topics)?;
        if left_rows.is_empty() {
            return Ok(vec![]);
        }
        return pipeline.process(left_rows);
    }

    let rows = consume_all(broker, &pipeline.source_topics)?;
    if rows.is_empty() {
        return Ok(vec![]);
    }
    pipeline.process(rows)
}

/// Consume all available records from the given topics.
fn consume_all(broker: &BrokerRef, topics: &[String]) -> Result<Vec<Row>> {
    let topic_refs: Vec<&str> = topics.iter().map(|s| s.as_str()).collect();

    let mut consumer = merkql::broker::Broker::consumer(
        broker,
        ConsumerConfig {
            group_id: format!("_merksql_pull_{}", uuid::Uuid::new_v4()),
            auto_commit: false,
            offset_reset: OffsetReset::Earliest,
        },
    );
    consumer.subscribe(&topic_refs)?;
    let records = consumer.poll(Duration::from_millis(100))?;
    consumer.close()?;

    Ok(records
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
        .collect())
}
