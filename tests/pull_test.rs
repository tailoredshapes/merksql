use merkql::broker::{Broker, BrokerConfig};
use merkql::record::ProducerRecord;

use merksql::builder::*;
use merksql::runtime::pull;
use merksql::schema::SchemaRegistry;
use merksql::types::*;

fn setup_broker(dir: &tempfile::TempDir) -> merkql::broker::BrokerRef {
    Broker::open(BrokerConfig::new(dir.path())).unwrap()
}

fn create_registry(topic: &str) -> SchemaRegistry {
    let mut registry = SchemaRegistry::new();
    registry
        .register_stream(
            "events",
            Schema::new(vec![
                Column::new("user_id", DataType::String),
                Column::new("action", DataType::String),
                Column::new("value", DataType::Integer),
            ]),
            topic,
        )
        .unwrap();
    registry
}

fn produce_events(broker: &merkql::broker::BrokerRef, topic: &str) {
    let producer = Broker::producer(broker);
    let events = vec![
        r#"{"user_id": "u1", "action": "click", "value": 1}"#,
        r#"{"user_id": "u2", "action": "view", "value": 5}"#,
        r#"{"user_id": "u1", "action": "purchase", "value": 100}"#,
        r#"{"user_id": "u3", "action": "click", "value": 1}"#,
        r#"{"user_id": "u2", "action": "purchase", "value": 50}"#,
        r#"{"user_id": "u1", "action": "click", "value": 1}"#,
    ];
    for event in events {
        producer
            .send(&ProducerRecord::new(topic, None, event))
            .unwrap();
    }
}

#[test]
fn pull_query_scan() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "test-pull-scan";
    produce_events(&broker, topic);

    let registry = create_registry(topic);
    let plan = QueryBuilder::from_source("events").build();
    let result = pull::pull_query(&broker, &plan, &registry).unwrap();

    assert_eq!(result.len(), 6);
}

#[test]
fn pull_query_filter() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "test-pull-filter";
    produce_events(&broker, topic);

    let registry = create_registry(topic);
    let plan = QueryBuilder::from_source("events")
        .filter(col("action").eq_expr(lit_str("purchase")))
        .build();
    let result = pull::pull_query(&broker, &plan, &registry).unwrap();

    assert_eq!(result.len(), 2);
    for row in &result {
        assert_eq!(row.get(1), &Value::String("purchase".to_string()));
    }
}

#[test]
fn pull_query_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "test-pull-agg";
    produce_events(&broker, topic);

    let registry = create_registry(topic);
    let plan = QueryBuilder::from_source("events")
        .group_by(&[col("user_id")])
        .count_star("cnt")
        .sum(col("value"), "total_value")
        .build();
    let result = pull::pull_query(&broker, &plan, &registry).unwrap();

    assert_eq!(result.len(), 3);

    let mut results: Vec<(String, i64, f64)> = result
        .iter()
        .map(|r| {
            let uid = r.get(0).as_str().unwrap().to_string();
            let cnt = r.get(1).as_i64().unwrap();
            let total = r.get(2).as_f64().unwrap();
            (uid, cnt, total)
        })
        .collect();
    results.sort_by(|a, b| a.0.cmp(&b.0));

    // u1: 3 events, total=102
    assert_eq!(results[0].0, "u1");
    assert_eq!(results[0].1, 3);
    assert!((results[0].2 - 102.0).abs() < 0.01);

    // u2: 2 events, total=55
    assert_eq!(results[1].0, "u2");
    assert_eq!(results[1].1, 2);
    assert!((results[1].2 - 55.0).abs() < 0.01);

    // u3: 1 event, total=1
    assert_eq!(results[2].0, "u3");
    assert_eq!(results[2].1, 1);
    assert!((results[2].2 - 1.0).abs() < 0.01);
}

#[test]
fn pull_query_empty_topic() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "test-pull-empty";
    // Don't produce any events

    // Ensure topic exists
    broker.ensure_topic(topic).unwrap();

    let registry = create_registry(topic);
    let plan = QueryBuilder::from_source("events").build();
    let result = pull::pull_query(&broker, &plan, &registry).unwrap();

    assert_eq!(result.len(), 0);
}

#[test]
fn pull_query_filter_then_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "test-pull-fa";
    produce_events(&broker, topic);

    let registry = create_registry(topic);
    // Count only click events per user
    let plan = QueryBuilder::from_source("events")
        .filter(col("action").eq_expr(lit_str("click")))
        .group_by(&[col("user_id")])
        .count_star("click_count")
        .build();
    let result = pull::pull_query(&broker, &plan, &registry).unwrap();

    let mut results: Vec<(String, i64)> = result
        .iter()
        .map(|r| {
            let uid = r.get(0).as_str().unwrap().to_string();
            let cnt = r.get(1).as_i64().unwrap();
            (uid, cnt)
        })
        .collect();
    results.sort();

    // u1: 2 clicks, u3: 1 click
    assert_eq!(results, vec![("u1".to_string(), 2), ("u3".to_string(), 1),]);
}

#[test]
fn pull_query_project_and_filter() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "test-pull-pf";
    produce_events(&broker, topic);

    let registry = create_registry(topic);
    let plan = QueryBuilder::from_source("events")
        .filter(col("value").gt(lit_i64(10)))
        .select(&[col("user_id"), col("value")])
        .build();
    let result = pull::pull_query(&broker, &plan, &registry).unwrap();

    // Only purchases (value=100, value=50)
    assert_eq!(result.len(), 2);
    for row in &result {
        assert_eq!(row.values.len(), 2);
        assert!(row.get(1).as_i64().unwrap() > 10);
    }
}
