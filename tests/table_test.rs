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

fn create_registry(topic: &str) -> SchemaRegistry {
    let mut registry = SchemaRegistry::new();
    registry
        .register_stream(
            "orders",
            Schema::new(vec![
                Column::new("customer_id", DataType::String),
                Column::new("product", DataType::String),
                Column::new("amount", DataType::Double),
                Column::new("quantity", DataType::Integer),
            ]),
            topic,
        )
        .unwrap();
    registry
}

fn produce_orders(broker: &merkql::broker::BrokerRef, topic: &str) {
    let producer = Broker::producer(broker);
    let orders = vec![
        r#"{"customer_id": "c1", "product": "widget", "amount": 10.50, "quantity": 2}"#,
        r#"{"customer_id": "c2", "product": "gadget", "amount": 25.00, "quantity": 1}"#,
        r#"{"customer_id": "c1", "product": "gadget", "amount": 25.00, "quantity": 3}"#,
        r#"{"customer_id": "c3", "product": "widget", "amount": 10.50, "quantity": 5}"#,
        r#"{"customer_id": "c2", "product": "widget", "amount": 10.50, "quantity": 1}"#,
        r#"{"customer_id": "c1", "product": "widget", "amount": 10.50, "quantity": 1}"#,
    ];
    for order in orders {
        producer
            .send(&ProducerRecord::new(topic, None, order))
            .unwrap();
    }
}

#[test]
fn aggregate_count_by_customer() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "test-orders-count";
    produce_orders(&broker, topic);

    let registry = create_registry(topic);
    let plan = QueryBuilder::from_source("orders")
        .group_by(&[col("customer_id")])
        .count_star("order_count")
        .build();

    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();
    let rows = consume_as_rows(&broker, topic);
    let result = pipeline.process(rows).unwrap();

    // 3 customers: c1 (3 orders), c2 (2 orders), c3 (1 order)
    assert_eq!(result.len(), 3);

    // Sort by customer_id for deterministic assertions
    let mut results: Vec<(String, i64)> = result
        .iter()
        .map(|r| {
            let cid = r.get(0).as_str().unwrap().to_string();
            let cnt = r.get(1).as_i64().unwrap();
            (cid, cnt)
        })
        .collect();
    results.sort();

    assert_eq!(
        results,
        vec![
            ("c1".to_string(), 3),
            ("c2".to_string(), 2),
            ("c3".to_string(), 1),
        ]
    );
}

#[test]
fn aggregate_sum_amount() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "test-orders-sum";
    produce_orders(&broker, topic);

    let registry = create_registry(topic);
    let plan = QueryBuilder::from_source("orders")
        .group_by(&[col("customer_id")])
        .sum(col("amount"), "total_amount")
        .build();

    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();
    let rows = consume_as_rows(&broker, topic);
    let result = pipeline.process(rows).unwrap();

    assert_eq!(result.len(), 3);

    let mut results: Vec<(String, f64)> = result
        .iter()
        .map(|r| {
            let cid = r.get(0).as_str().unwrap().to_string();
            let total = r.get(1).as_f64().unwrap();
            (cid, total)
        })
        .collect();
    results.sort_by(|a, b| a.0.cmp(&b.0));

    // c1: 10.50 + 25.00 + 10.50 = 46.00
    assert!((results[0].1 - 46.0).abs() < 0.01);
    // c2: 25.00 + 10.50 = 35.50
    assert!((results[1].1 - 35.5).abs() < 0.01);
    // c3: 10.50
    assert!((results[2].1 - 10.5).abs() < 0.01);
}

#[test]
fn aggregate_avg() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "test-orders-avg";
    produce_orders(&broker, topic);

    let registry = create_registry(topic);
    let plan = QueryBuilder::from_source("orders")
        .group_by(&[col("customer_id")])
        .avg(col("amount"), "avg_amount")
        .build();

    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();
    let rows = consume_as_rows(&broker, topic);
    let result = pipeline.process(rows).unwrap();

    let mut results: Vec<(String, f64)> = result
        .iter()
        .map(|r| {
            let cid = r.get(0).as_str().unwrap().to_string();
            let avg = r.get(1).as_f64().unwrap();
            (cid, avg)
        })
        .collect();
    results.sort_by(|a, b| a.0.cmp(&b.0));

    // c1: (10.50 + 25.00 + 10.50) / 3 ≈ 15.33
    assert!((results[0].1 - 15.333).abs() < 0.01);
}

#[test]
fn aggregate_min_max() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "test-orders-minmax";
    produce_orders(&broker, topic);

    let registry = create_registry(topic);
    let plan = QueryBuilder::from_source("orders")
        .group_by(&[col("customer_id")])
        .min(col("amount"), "min_amount")
        .max(col("amount"), "max_amount")
        .build();

    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();
    let rows = consume_as_rows(&broker, topic);
    let result = pipeline.process(rows).unwrap();

    let mut results: Vec<(String, f64, f64)> = result
        .iter()
        .map(|r| {
            let cid = r.get(0).as_str().unwrap().to_string();
            let min = r.get(1).as_f64().unwrap();
            let max = r.get(2).as_f64().unwrap();
            (cid, min, max)
        })
        .collect();
    results.sort_by(|a, b| a.0.cmp(&b.0));

    // c1: min=10.50, max=25.00
    assert!((results[0].1 - 10.5).abs() < 0.01);
    assert!((results[0].2 - 25.0).abs() < 0.01);
}

#[test]
fn aggregate_multiple_functions() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "test-orders-multi";
    produce_orders(&broker, topic);

    let registry = create_registry(topic);
    let plan = QueryBuilder::from_source("orders")
        .group_by(&[col("customer_id")])
        .count_star("cnt")
        .sum(col("amount"), "total")
        .avg(col("amount"), "avg")
        .min(col("amount"), "min")
        .max(col("amount"), "max")
        .build();

    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();
    let rows = consume_as_rows(&broker, topic);
    let result = pipeline.process(rows).unwrap();

    // Find c1
    let c1 = result
        .iter()
        .find(|r| r.get(0) == &Value::String("c1".to_string()))
        .unwrap();

    assert_eq!(c1.get(1), &Value::Integer(3)); // count
    assert!((c1.get(2).as_f64().unwrap() - 46.0).abs() < 0.01); // sum
    assert!((c1.get(3).as_f64().unwrap() - 15.333).abs() < 0.01); // avg
    assert_eq!(c1.get(4), &Value::Double(10.5)); // min
    assert_eq!(c1.get(5), &Value::Double(25.0)); // max
}

#[test]
fn aggregate_with_filter() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "test-orders-filter-agg";
    produce_orders(&broker, topic);

    let registry = create_registry(topic);
    // Count only widget orders per customer
    let plan = QueryBuilder::from_source("orders")
        .filter(col("product").eq_expr(lit_str("widget")))
        .group_by(&[col("customer_id")])
        .count_star("widget_count")
        .build();

    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();
    let rows = consume_as_rows(&broker, topic);
    let result = pipeline.process(rows).unwrap();

    let mut results: Vec<(String, i64)> = result
        .iter()
        .map(|r| {
            let cid = r.get(0).as_str().unwrap().to_string();
            let cnt = r.get(1).as_i64().unwrap();
            (cid, cnt)
        })
        .collect();
    results.sort();

    // c1: 2 widgets, c2: 1 widget, c3: 1 widget
    assert_eq!(
        results,
        vec![
            ("c1".to_string(), 2),
            ("c2".to_string(), 1),
            ("c3".to_string(), 1),
        ]
    );
}

#[test]
fn aggregate_having() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "test-orders-having";
    produce_orders(&broker, topic);

    let registry = create_registry(topic);
    // Only customers with > 1 order
    let plan = QueryBuilder::from_source("orders")
        .group_by(&[col("customer_id")])
        .count_star("order_count")
        .having(col("order_count").gt(lit_i64(1)))
        .build();

    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();
    let rows = consume_as_rows(&broker, topic);
    let result = pipeline.process(rows).unwrap();

    // Only c1 (3) and c2 (2) should pass
    assert_eq!(result.len(), 2);

    let mut cids: Vec<String> = result
        .iter()
        .map(|r| r.get(0).as_str().unwrap().to_string())
        .collect();
    cids.sort();
    assert_eq!(cids, vec!["c1", "c2"]);
}

#[test]
fn aggregate_output_schema() {
    let dir = tempfile::tempdir().unwrap();
    let _broker = setup_broker(&dir);

    let registry = create_registry("test-topic");
    let plan = QueryBuilder::from_source("orders")
        .group_by(&[col("customer_id")])
        .count_star("cnt")
        .sum(col("amount"), "total")
        .build();

    let pipeline = pipeline::compile(&plan, &registry).unwrap();
    assert_eq!(pipeline.output_schema.len(), 3);
    assert_eq!(pipeline.output_schema.columns[0].name, "customer_id");
    assert_eq!(
        pipeline.output_schema.columns[0].data_type,
        DataType::String
    );
    assert_eq!(pipeline.output_schema.columns[1].name, "cnt");
    assert_eq!(
        pipeline.output_schema.columns[1].data_type,
        DataType::Integer
    );
    assert_eq!(pipeline.output_schema.columns[2].name, "total");
    assert_eq!(
        pipeline.output_schema.columns[2].data_type,
        DataType::Double
    );
}

#[test]
fn incremental_aggregation() {
    // Test that processing multiple batches accumulates correctly
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "test-orders-incr";

    let producer = Broker::producer(&broker);
    let registry = create_registry(topic);
    let plan = QueryBuilder::from_source("orders")
        .group_by(&[col("customer_id")])
        .count_star("cnt")
        .build();
    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();

    // Batch 1
    producer
        .send(&ProducerRecord::new(
            topic,
            None,
            r#"{"customer_id": "c1", "product": "x", "amount": 1.0, "quantity": 1}"#,
        ))
        .unwrap();
    producer
        .send(&ProducerRecord::new(
            topic,
            None,
            r#"{"customer_id": "c2", "product": "x", "amount": 1.0, "quantity": 1}"#,
        ))
        .unwrap();

    let rows1 = consume_as_rows(&broker, topic);
    let result1 = pipeline.process(rows1).unwrap();
    assert_eq!(result1.len(), 2);

    // Batch 2
    producer
        .send(&ProducerRecord::new(
            topic,
            None,
            r#"{"customer_id": "c1", "product": "y", "amount": 2.0, "quantity": 1}"#,
        ))
        .unwrap();

    let mut consumer = Broker::consumer(
        &broker,
        ConsumerConfig {
            group_id: format!("test-{}", uuid::Uuid::new_v4()),
            auto_commit: false,
            offset_reset: OffsetReset::Latest,
        },
    );
    // We need to re-read from the topic; use a new consumer group reading from offset 2
    // Actually, let's just create rows manually for batch 2
    let batch2_rows = vec![Row::with_metadata(
        vec![Value::String(
            r#"{"customer_id": "c1", "product": "y", "amount": 2.0, "quantity": 1}"#.to_string(),
        )],
        RowMetadata::default(),
    )];
    consumer.close().unwrap();

    let result2 = pipeline.process(batch2_rows).unwrap();
    // Should show c1 with count 2 (1 from batch1 + 1 from batch2) and c2 with count 1
    assert_eq!(result2.len(), 2);

    let c1 = result2
        .iter()
        .find(|r| r.get(0) == &Value::String("c1".to_string()))
        .unwrap();
    assert_eq!(c1.get(1), &Value::Integer(2));

    let c2 = result2
        .iter()
        .find(|r| r.get(0) == &Value::String("c2".to_string()))
        .unwrap();
    assert_eq!(c2.get(1), &Value::Integer(1));
}
