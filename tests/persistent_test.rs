use std::thread;
use std::time::Duration;

use merkql::broker::{Broker, BrokerConfig};
use merkql::consumer::{ConsumerConfig, OffsetReset};
use merkql::record::ProducerRecord;

use merksql::builder::*;
use merksql::runtime::persistent::{PersistentQuery, QueryStatus};
use merksql::runtime::registry::QueryRegistry;
use merksql::schema::SchemaRegistry;
use merksql::types::*;
use merksql::{ExecuteResult, MerkSql};

fn setup_broker(dir: &tempfile::TempDir) -> merkql::broker::BrokerRef {
    Broker::open(BrokerConfig::new(dir.path())).unwrap()
}

fn create_registry(input_topic: &str) -> SchemaRegistry {
    let mut registry = SchemaRegistry::new();
    registry
        .register_stream(
            "readings",
            Schema::new(vec![
                Column::new("sensor_id", DataType::String),
                Column::new("temp", DataType::Double),
                Column::new("humidity", DataType::Integer),
            ]),
            input_topic,
        )
        .unwrap();
    registry
}

fn read_output_topic(broker: &merkql::broker::BrokerRef, topic: &str) -> Vec<String> {
    let mut consumer = Broker::consumer(
        broker,
        ConsumerConfig {
            group_id: format!("test-reader-{}", uuid::Uuid::new_v4()),
            auto_commit: false,
            offset_reset: OffsetReset::Earliest,
        },
    );
    consumer.subscribe(&[topic]).unwrap();
    let records = consumer.poll(Duration::from_millis(100)).unwrap();
    consumer.close().unwrap();
    records.into_iter().map(|r| r.value).collect()
}

#[test]
fn persistent_query_processes_records() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let input_topic = "persistent-input";
    let output_topic = "persistent-output";

    // Produce records BEFORE starting the query
    let producer = Broker::producer(&broker);
    producer
        .send(&ProducerRecord::new(
            input_topic,
            None,
            r#"{"sensor_id": "s1", "temp": 22.5, "humidity": 45}"#,
        ))
        .unwrap();
    producer
        .send(&ProducerRecord::new(
            input_topic,
            None,
            r#"{"sensor_id": "s2", "temp": 105.3, "humidity": 80}"#,
        ))
        .unwrap();
    producer
        .send(&ProducerRecord::new(
            input_topic,
            None,
            r#"{"sensor_id": "s3", "temp": 110.0, "humidity": 90}"#,
        ))
        .unwrap();

    let registry = create_registry(input_topic);

    // Create a persistent query: filter temp > 100
    let plan = QueryBuilder::from_source("readings")
        .filter(col("temp").gt(lit_f64(100.0)))
        .select(&[col("sensor_id"), col("temp")])
        .as_stream("high_temps", output_topic)
        .build();

    let mut query = PersistentQuery::start(
        "test-q1".to_string(),
        plan,
        output_topic.to_string(),
        broker.clone(),
        &registry,
    )
    .unwrap();

    assert_eq!(query.status(), QueryStatus::Running);

    // Wait for processing
    thread::sleep(Duration::from_millis(500));

    // Stop the query
    query.stop();
    assert_ne!(query.status(), QueryStatus::Running);

    // Read output topic
    let output = read_output_topic(&broker, output_topic);
    // Should have 2 records (temp > 100: s2 and s3)
    assert_eq!(output.len(), 2);
}

#[test]
fn persistent_query_terminate() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let input_topic = "term-input";
    let output_topic = "term-output";

    let registry = create_registry(input_topic);
    broker.ensure_topic(input_topic).unwrap();

    let plan = QueryBuilder::from_source("readings")
        .as_stream("out", output_topic)
        .build();

    let mut query = PersistentQuery::start(
        "test-q2".to_string(),
        plan,
        output_topic.to_string(),
        broker.clone(),
        &registry,
    )
    .unwrap();

    assert_eq!(query.status(), QueryStatus::Running);

    // Terminate immediately
    query.terminate();
    assert_eq!(query.status(), QueryStatus::Terminated);
}

#[test]
fn query_registry_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let input_topic = "reg-input";
    let output_topic = "reg-output";

    let registry = create_registry(input_topic);
    broker.ensure_topic(input_topic).unwrap();

    let mut query_reg = QueryRegistry::new();

    let plan = QueryBuilder::from_source("readings")
        .as_stream("out", output_topic)
        .build();

    let id = query_reg
        .start_query(plan, output_topic.to_string(), &broker, &registry)
        .unwrap();

    assert_eq!(id, "q1");
    assert_eq!(query_reg.len(), 1);

    let status = query_reg.status(&id).unwrap();
    assert_eq!(status, QueryStatus::Running);

    let listing = query_reg.list();
    assert_eq!(listing.len(), 1);
    assert_eq!(listing[0].0, "q1");

    // Stop
    query_reg.stop(&id).unwrap();
    let status = query_reg.status(&id).unwrap();
    assert_ne!(status, QueryStatus::Running);

    // Unknown query
    assert!(query_reg.status("nonexistent").is_none());
    assert!(query_reg.stop("nonexistent").is_err());
}

#[test]
fn query_registry_multiple_queries() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let input_topic = "multi-input";

    let registry = create_registry(input_topic);
    broker.ensure_topic(input_topic).unwrap();

    let mut query_reg = QueryRegistry::new();

    let plan1 = QueryBuilder::from_source("readings")
        .as_stream("out1", "output1")
        .build();
    let plan2 = QueryBuilder::from_source("readings")
        .as_stream("out2", "output2")
        .build();

    let id1 = query_reg
        .start_query(plan1, "output1".to_string(), &broker, &registry)
        .unwrap();
    let id2 = query_reg
        .start_query(plan2, "output2".to_string(), &broker, &registry)
        .unwrap();

    assert_eq!(id1, "q1");
    assert_eq!(id2, "q2");
    assert_eq!(query_reg.len(), 2);

    // Stop all
    query_reg.stop_all();
    assert_ne!(query_reg.status(&id1).unwrap(), QueryStatus::Running);
    assert_ne!(query_reg.status(&id2).unwrap(), QueryStatus::Running);
}

#[test]
fn merksql_execute_ddl() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let mut engine = MerkSql::new(broker);

    let result = engine.execute(
        "CREATE TABLE readings (sensor_id VARCHAR, temp DOUBLE) WITH (KAFKA_TOPIC='readings-topic')"
    ).unwrap();

    match result {
        ExecuteResult::SourceCreated { name } => {
            assert_eq!(name, "readings");
        }
        _ => panic!("Expected SourceCreated"),
    }

    assert!(engine.schemas.get("readings").is_some());
}

#[test]
fn merksql_execute_pull_query() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "merksql-pull";

    let producer = Broker::producer(&broker);
    producer
        .send(&ProducerRecord::new(
            topic,
            None,
            r#"{"sensor_id": "s1", "temp": 22.5}"#,
        ))
        .unwrap();
    producer
        .send(&ProducerRecord::new(
            topic,
            None,
            r#"{"sensor_id": "s2", "temp": 105.0}"#,
        ))
        .unwrap();

    let mut engine = MerkSql::new(broker);
    engine
        .execute(&format!(
            "CREATE TABLE readings (sensor_id VARCHAR, temp DOUBLE) WITH (KAFKA_TOPIC='{}')",
            topic
        ))
        .unwrap();

    let result = engine
        .execute("SELECT sensor_id, temp FROM readings WHERE temp > 100.0")
        .unwrap();
    match result {
        ExecuteResult::Rows { rows, schema } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get(0), &Value::String("s2".to_string()));
            assert_eq!(schema.len(), 2);
        }
        _ => panic!("Expected Rows"),
    }
}

#[test]
fn merksql_execute_persistent_query() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let input_topic = "merksql-input";
    let output_topic = "merksql-output";

    let producer = Broker::producer(&broker);
    producer
        .send(&ProducerRecord::new(
            input_topic,
            None,
            r#"{"sensor_id": "s1", "temp": 150.0}"#,
        ))
        .unwrap();

    let mut engine = MerkSql::new(broker.clone());
    engine
        .execute(&format!(
            "CREATE TABLE readings (sensor_id VARCHAR, temp DOUBLE) WITH (KAFKA_TOPIC='{}')",
            input_topic
        ))
        .unwrap();

    let result = engine.execute(
        &format!("CREATE TABLE high_temps WITH (KAFKA_TOPIC='{}') AS SELECT sensor_id, temp FROM readings WHERE temp > 100.0", output_topic)
    ).unwrap();

    match result {
        ExecuteResult::QueryStarted { id } => {
            assert_eq!(id, "q1");

            // Wait for processing
            thread::sleep(Duration::from_millis(500));

            // Verify query is tracked
            let status = engine.queries.status(&id).unwrap();
            assert_eq!(status, QueryStatus::Running);

            // Stop it
            engine.queries.stop(&id).unwrap();
        }
        _ => panic!("Expected QueryStarted"),
    }

    // Verify output exists
    let output = read_output_topic(&broker, output_topic);
    assert_eq!(output.len(), 1);
}

#[test]
fn merksql_builder_api() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "builder-pull";

    let producer = Broker::producer(&broker);
    producer
        .send(&ProducerRecord::new(topic, None, r#"{"x": 10, "y": 20}"#))
        .unwrap();
    producer
        .send(&ProducerRecord::new(topic, None, r#"{"x": 30, "y": 40}"#))
        .unwrap();

    let mut engine = MerkSql::new(broker);
    engine
        .schemas
        .register_stream(
            "data",
            Schema::new(vec![
                Column::new("x", DataType::Integer),
                Column::new("y", DataType::Integer),
            ]),
            topic,
        )
        .unwrap();

    let plan = QueryBuilder::from_source("data")
        .filter(col("x").gt(lit_i64(15)))
        .select(&[col("x"), col("y")])
        .build();

    let result = engine.query(plan).unwrap();
    match result {
        ExecuteResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].get(0), &Value::Integer(30));
            assert_eq!(rows[0].get(1), &Value::Integer(40));
        }
        _ => panic!("Expected Rows"),
    }
}
