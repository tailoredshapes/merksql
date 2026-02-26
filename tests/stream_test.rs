use std::time::Duration;

use merkql::broker::{Broker, BrokerConfig};
use merkql::consumer::{ConsumerConfig, OffsetReset};
use merkql::record::ProducerRecord;

use merksql::builder::*;
use merksql::engine::pipeline;
use merksql::schema::SchemaRegistry;
use merksql::types::*;

fn setup_broker(dir: &tempfile::TempDir) -> merkql::broker::BrokerRef {
    Broker::open(BrokerConfig::new(dir.path())).unwrap()
}

fn produce_readings(broker: &merkql::broker::BrokerRef, topic: &str) {
    let producer = Broker::producer(broker);
    let readings = vec![
        r#"{"sensor_id": "s1", "temp": 22.5, "humidity": 45}"#,
        r#"{"sensor_id": "s2", "temp": 105.3, "humidity": 80}"#,
        r#"{"sensor_id": "s1", "temp": 98.7, "humidity": 50}"#,
        r#"{"sensor_id": "s3", "temp": 110.0, "humidity": 90}"#,
        r#"{"sensor_id": "s2", "temp": 99.1, "humidity": 70}"#,
    ];
    for reading in readings {
        producer
            .send(&ProducerRecord::new(topic, None, reading))
            .unwrap();
    }
}

fn create_registry(topic: &str) -> SchemaRegistry {
    let mut registry = SchemaRegistry::new();
    registry
        .register_stream(
            "readings",
            Schema::new(vec![
                Column::new("sensor_id", DataType::String),
                Column::new("temp", DataType::Double),
                Column::new("humidity", DataType::Integer),
            ]),
            topic,
        )
        .unwrap();
    registry
}

/// Read raw records from a topic and convert to single-value rows for pipeline input.
fn consume_as_rows(broker: &merkql::broker::BrokerRef, topic: &str) -> Vec<Row> {
    let mut consumer = Broker::consumer(
        broker,
        ConsumerConfig {
            group_id: format!("test-{}", uuid::Uuid::new_v4()),
            auto_commit: false,
            offset_reset: OffsetReset::Earliest,
        },
    );
    consumer.subscribe(&[topic]).unwrap();
    let records = consumer.poll(Duration::from_millis(100)).unwrap();
    consumer.close().unwrap();

    records
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
        .collect()
}

#[test]
fn scan_all_records() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "test-readings";
    produce_readings(&broker, topic);

    let registry = create_registry(topic);
    let plan = QueryBuilder::from_source("readings").build();
    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();

    let rows = consume_as_rows(&broker, topic);
    assert_eq!(rows.len(), 5);

    let result = pipeline.process(rows).unwrap();
    assert_eq!(result.len(), 5);

    // Check that deserialization worked
    assert_eq!(result[0].get(0), &Value::String("s1".to_string()));
    assert_eq!(result[0].get(1), &Value::Double(22.5));
    assert_eq!(result[0].get(2), &Value::Integer(45));
}

#[test]
fn filter_high_temp() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "test-readings-filter";
    produce_readings(&broker, topic);

    let registry = create_registry(topic);
    let plan = QueryBuilder::from_source("readings")
        .filter(col("temp").gt(lit_f64(100.0)))
        .build();

    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();
    let rows = consume_as_rows(&broker, topic);
    let result = pipeline.process(rows).unwrap();

    // Only s2 (105.3) and s3 (110.0) should pass
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].get(0), &Value::String("s2".to_string()));
    assert_eq!(result[0].get(1), &Value::Double(105.3));
    assert_eq!(result[1].get(0), &Value::String("s3".to_string()));
    assert_eq!(result[1].get(1), &Value::Double(110.0));
}

#[test]
fn project_columns() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "test-readings-project";
    produce_readings(&broker, topic);

    let registry = create_registry(topic);
    let plan = QueryBuilder::from_source("readings")
        .select(&[col("sensor_id"), col("temp")])
        .build();

    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();
    let rows = consume_as_rows(&broker, topic);
    let result = pipeline.process(rows).unwrap();

    assert_eq!(result.len(), 5);
    // Each row should have exactly 2 values
    assert_eq!(result[0].values.len(), 2);
    assert_eq!(result[0].get(0), &Value::String("s1".to_string()));
    assert_eq!(result[0].get(1), &Value::Double(22.5));
}

#[test]
fn filter_then_project() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "test-readings-fp";
    produce_readings(&broker, topic);

    let registry = create_registry(topic);
    let plan = QueryBuilder::from_source("readings")
        .filter(col("temp").gt(lit_f64(100.0)))
        .select(&[col("sensor_id"), col("temp")])
        .build();

    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();
    let rows = consume_as_rows(&broker, topic);
    let result = pipeline.process(rows).unwrap();

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values.len(), 2);
    assert_eq!(result[0].get(0), &Value::String("s2".to_string()));
    assert_eq!(result[0].get(1), &Value::Double(105.3));
}

#[test]
fn project_with_expressions() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "test-readings-expr";
    produce_readings(&broker, topic);

    let registry = create_registry(topic);
    // SELECT sensor_id, temp * 1.8 + 32.0 AS temp_f
    let plan = QueryBuilder::from_source("readings")
        .select(&[
            col("sensor_id"),
            col("temp")
                .mul(lit_f64(1.8))
                .add(lit_f64(32.0))
                .alias("temp_f"),
        ])
        .build();

    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();
    let rows = consume_as_rows(&broker, topic);
    let result = pipeline.process(rows).unwrap();

    assert_eq!(result.len(), 5);
    // First row: 22.5 * 1.8 + 32.0 = 72.5
    let temp_f = result[0].get(1).as_f64().unwrap();
    assert!((temp_f - 72.5).abs() < 0.001);
}

#[test]
fn pipeline_output_schema() {
    let dir = tempfile::tempdir().unwrap();
    let _broker = setup_broker(&dir);

    let registry = create_registry("test-topic");
    let plan = QueryBuilder::from_source("readings")
        .select(&[col("sensor_id"), col("temp").alias("temperature")])
        .build();

    let pipeline = pipeline::compile(&plan, &registry).unwrap();
    assert_eq!(pipeline.output_schema.len(), 2);
    assert_eq!(pipeline.output_schema.columns[0].name, "sensor_id");
    assert_eq!(pipeline.output_schema.columns[1].name, "temperature");
}

#[test]
fn complex_filter_predicate() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "test-readings-complex";
    produce_readings(&broker, topic);

    let registry = create_registry(topic);
    // WHERE temp > 90 AND humidity > 60
    let plan = QueryBuilder::from_source("readings")
        .filter(
            col("temp")
                .gt(lit_f64(90.0))
                .and(col("humidity").gt(lit_i64(60))),
        )
        .build();

    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();
    let rows = consume_as_rows(&broker, topic);
    let result = pipeline.process(rows).unwrap();

    // s2: 105.3 / 80 ✓, s3: 110.0 / 90 ✓, s2: 99.1 / 70 ✓
    // s1: 22.5 / 45 ✗, s1: 98.7 / 50 ✗ (humidity <= 60)
    assert_eq!(result.len(), 3);
}

#[test]
fn empty_result_from_filter() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "test-readings-empty";
    produce_readings(&broker, topic);

    let registry = create_registry(topic);
    // No reading has temp > 1000
    let plan = QueryBuilder::from_source("readings")
        .filter(col("temp").gt(lit_f64(1000.0)))
        .build();

    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();
    let rows = consume_as_rows(&broker, topic);
    let result = pipeline.process(rows).unwrap();

    assert_eq!(result.len(), 0);
}

#[test]
fn malformed_json_skipped() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "test-malformed";

    let producer = Broker::producer(&broker);
    producer
        .send(&ProducerRecord::new(
            topic,
            None,
            r#"{"sensor_id": "s1", "temp": 50.0, "humidity": 30}"#,
        ))
        .unwrap();
    producer
        .send(&ProducerRecord::new(topic, None, "not valid json"))
        .unwrap();
    producer
        .send(&ProducerRecord::new(
            topic,
            None,
            r#"{"sensor_id": "s2", "temp": 60.0, "humidity": 40}"#,
        ))
        .unwrap();

    let registry = create_registry(topic);
    let plan = QueryBuilder::from_source("readings").build();
    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();
    let rows = consume_as_rows(&broker, topic);
    let result = pipeline.process(rows).unwrap();

    // Malformed JSON should be skipped
    assert_eq!(result.len(), 2);
}

#[test]
fn source_topics_resolved() {
    let dir = tempfile::tempdir().unwrap();
    let _broker = setup_broker(&dir);

    let registry = create_registry("my-readings-topic");
    let plan = QueryBuilder::from_source("readings").build();
    let pipeline = pipeline::compile(&plan, &registry).unwrap();

    assert_eq!(pipeline.source_topics, vec!["my-readings-topic"]);
}

#[test]
fn unknown_source_error() {
    let dir = tempfile::tempdir().unwrap();
    let _broker = setup_broker(&dir);

    let registry = SchemaRegistry::new();
    let plan = QueryBuilder::from_source("nonexistent").build();
    let result = pipeline::compile(&plan, &registry);

    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Unknown source: nonexistent")
    );
}
