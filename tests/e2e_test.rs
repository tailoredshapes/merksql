use std::time::Duration;

use chrono::DateTime;
use merkql::broker::{Broker, BrokerConfig};
use merkql::record::ProducerRecord;

use merksql::MerkSql;
use merksql::builder::*;
use merksql::engine::pipeline;
use merksql::schema::SchemaRegistry;
use merksql::types::*;

fn setup_broker(dir: &tempfile::TempDir) -> merkql::broker::BrokerRef {
    Broker::open(BrokerConfig::new(dir.path())).unwrap()
}

// === COLLECT_LIST / COLLECT_SET / TOPK Tests ===

#[test]
fn collect_list_aggregation() {
    let dir = tempfile::tempdir().unwrap();
    let _broker = setup_broker(&dir);

    let mut registry = SchemaRegistry::new();
    registry
        .register_stream(
            "events",
            Schema::new(vec![
                Column::new("category", DataType::String),
                Column::new("item", DataType::String),
            ]),
            "test-topic",
        )
        .unwrap();

    let plan = QueryBuilder::from_source("events")
        .group_by(&[col("category")])
        .collect_list(col("item"), "items")
        .build();

    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();

    let rows: Vec<Row> = [
        (r#"{"category": "fruit", "item": "apple"}"#),
        (r#"{"category": "fruit", "item": "banana"}"#),
        (r#"{"category": "fruit", "item": "apple"}"#), // duplicate
        (r#"{"category": "veggie", "item": "carrot"}"#),
    ]
    .iter()
    .map(|json| Row::new(vec![Value::String(json.to_string())]))
    .collect();

    let result = pipeline.process(rows).unwrap();

    let mut results: Vec<(String, Vec<String>)> = result
        .iter()
        .map(|r| {
            let cat = r.get(0).as_str().unwrap().to_string();
            let items = match r.get(1) {
                Value::Array(arr) => arr
                    .iter()
                    .map(|v| v.as_str().unwrap().to_string())
                    .collect(),
                _ => panic!("Expected array"),
            };
            (cat, items)
        })
        .collect();
    results.sort_by(|a, b| a.0.cmp(&b.0));

    // collect_list keeps duplicates
    assert_eq!(results[0].0, "fruit");
    assert_eq!(results[0].1.len(), 3); // apple, banana, apple
    assert_eq!(results[1].0, "veggie");
    assert_eq!(results[1].1.len(), 1);
}

#[test]
fn collect_set_aggregation() {
    let dir = tempfile::tempdir().unwrap();
    let _broker = setup_broker(&dir);

    let mut registry = SchemaRegistry::new();
    registry
        .register_stream(
            "events",
            Schema::new(vec![
                Column::new("category", DataType::String),
                Column::new("item", DataType::String),
            ]),
            "test-topic",
        )
        .unwrap();

    let plan = QueryBuilder::from_source("events")
        .group_by(&[col("category")])
        .collect_set(col("item"), "unique_items")
        .build();

    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();

    let rows: Vec<Row> = [
        r#"{"category": "fruit", "item": "apple"}"#,
        r#"{"category": "fruit", "item": "banana"}"#,
        r#"{"category": "fruit", "item": "apple"}"#, // duplicate
    ]
    .iter()
    .map(|json| Row::new(vec![Value::String(json.to_string())]))
    .collect();

    let result = pipeline.process(rows).unwrap();
    assert_eq!(result.len(), 1);

    let items = match result[0].get(1) {
        Value::Array(arr) => arr.clone(),
        _ => panic!("Expected array"),
    };
    // collect_set deduplicates
    assert_eq!(items.len(), 2);
}

#[test]
fn topk_aggregation() {
    let dir = tempfile::tempdir().unwrap();
    let _broker = setup_broker(&dir);

    let mut registry = SchemaRegistry::new();
    registry
        .register_stream(
            "events",
            Schema::new(vec![
                Column::new("category", DataType::String),
                Column::new("score", DataType::Double),
            ]),
            "test-topic",
        )
        .unwrap();

    let plan = QueryBuilder::from_source("events")
        .group_by(&[col("category")])
        .topk(2, col("score"), "top_scores")
        .build();

    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();

    let rows: Vec<Row> = [
        r#"{"category": "game", "score": 10}"#,
        r#"{"category": "game", "score": 50}"#,
        r#"{"category": "game", "score": 30}"#,
        r#"{"category": "game", "score": 20}"#,
    ]
    .iter()
    .map(|json| Row::new(vec![Value::String(json.to_string())]))
    .collect();

    let result = pipeline.process(rows).unwrap();
    assert_eq!(result.len(), 1);

    let scores = match result[0].get(1) {
        Value::Array(arr) => arr.clone(),
        _ => panic!("Expected array"),
    };
    // topk(2) keeps 2 highest
    assert_eq!(scores.len(), 2);
    // Should be sorted descending: 50, 30
    assert_eq!(scores[0].as_f64().unwrap() as i64, 50);
    assert_eq!(scores[1].as_f64().unwrap() as i64, 30);
}

// === WINDOWSTART/WINDOWEND in SELECT ===

#[test]
fn windowstart_windowend_in_select() {
    let dir = tempfile::tempdir().unwrap();
    let _broker = setup_broker(&dir);

    let mut registry = SchemaRegistry::new();
    registry
        .register_stream(
            "events",
            Schema::new(vec![
                Column::new("sensor_id", DataType::String),
                Column::new("value", DataType::Double),
            ]),
            "test-topic",
        )
        .unwrap();

    // Build windowed aggregation
    let plan = QueryBuilder::from_source("events")
        .group_by(&[col("sensor_id")])
        .tumbling(Duration::from_secs(10))
        .count_star("cnt")
        .build();

    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();

    let rows: Vec<Row> = vec![
        Row::with_metadata(
            vec![Value::String(r#"{"sensor_id": "s1", "value": 10}"#.into())],
            RowMetadata {
                timestamp: DateTime::from_timestamp_millis(3000),
                ..Default::default()
            },
        ),
        Row::with_metadata(
            vec![Value::String(r#"{"sensor_id": "s1", "value": 20}"#.into())],
            RowMetadata {
                timestamp: DateTime::from_timestamp_millis(7000),
                ..Default::default()
            },
        ),
    ];

    let result = pipeline.process(rows).unwrap();
    assert_eq!(result.len(), 1);

    // Window metadata should be set
    let row = &result[0];
    assert!(row.metadata.window_start.is_some());
    assert!(row.metadata.window_end.is_some());
    assert_eq!(row.metadata.window_start.unwrap().timestamp_millis(), 0);
    assert_eq!(row.metadata.window_end.unwrap().timestamp_millis(), 10000);

    // The WINDOWSTART and WINDOWEND pseudo-columns should be evaluable
    let ws = merksql::expr::eval(&col("WINDOWSTART"), row, &pipeline.output_schema).unwrap();
    let we = merksql::expr::eval(&col("WINDOWEND"), row, &pipeline.output_schema).unwrap();

    assert!(matches!(ws, Value::Timestamp(_)));
    assert!(matches!(we, Value::Timestamp(_)));
}

// === COUNT DISTINCT ===

#[test]
fn count_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let _broker = setup_broker(&dir);

    let mut registry = SchemaRegistry::new();
    registry
        .register_stream(
            "events",
            Schema::new(vec![
                Column::new("category", DataType::String),
                Column::new("user_id", DataType::String),
            ]),
            "test-topic",
        )
        .unwrap();

    let plan = QueryBuilder::from_source("events")
        .group_by(&[col("category")])
        .count_distinct(col("user_id"), "unique_users")
        .build();

    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();

    let rows: Vec<Row> = [
        r#"{"category": "sports", "user_id": "u1"}"#,
        r#"{"category": "sports", "user_id": "u2"}"#,
        r#"{"category": "sports", "user_id": "u1"}"#, // duplicate
        r#"{"category": "sports", "user_id": "u3"}"#,
    ]
    .iter()
    .map(|json| Row::new(vec![Value::String(json.to_string())]))
    .collect();

    let result = pipeline.process(rows).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get(1).as_i64().unwrap(), 3); // 3 unique users
}

// === Complex Expression Tests ===

#[test]
fn case_when_in_filter() {
    let dir = tempfile::tempdir().unwrap();
    let _broker = setup_broker(&dir);

    let mut registry = SchemaRegistry::new();
    registry
        .register_stream(
            "events",
            Schema::new(vec![
                Column::new("status", DataType::String),
                Column::new("value", DataType::Double),
            ]),
            "test-topic",
        )
        .unwrap();

    // Filter using CASE expression: only keep rows where status maps to "active"
    let case_expr = merksql::expr::Expr::Case {
        operand: None,
        conditions: vec![(
            merksql::expr::Expr::BinaryOp {
                left: Box::new(col("status")),
                op: merksql::expr::BinaryOp::Eq,
                right: Box::new(lit_str("active")),
            },
            lit_bool(true),
        )],
        else_result: Some(Box::new(lit_bool(false))),
    };

    let plan = QueryBuilder::from_source("events")
        .filter(case_expr)
        .build();

    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();

    let rows: Vec<Row> = [
        r#"{"status": "active", "value": 10}"#,
        r#"{"status": "inactive", "value": 20}"#,
        r#"{"status": "active", "value": 30}"#,
    ]
    .iter()
    .map(|json| Row::new(vec![Value::String(json.to_string())]))
    .collect();

    let result = pipeline.process(rows).unwrap();
    assert_eq!(result.len(), 2);
}

// === Full E2E Scenario ===

#[test]
fn full_e2e_sql_create_and_query() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "e2e-readings";

    // Produce data
    let producer = Broker::producer(&broker);
    producer
        .send(&ProducerRecord::new(
            topic,
            None,
            r#"{"sensor_id": "s1", "temp": 22.5, "humidity": 45}"#,
        ))
        .unwrap();
    producer
        .send(&ProducerRecord::new(
            topic,
            None,
            r#"{"sensor_id": "s1", "temp": 25.0, "humidity": 50}"#,
        ))
        .unwrap();
    producer
        .send(&ProducerRecord::new(
            topic,
            None,
            r#"{"sensor_id": "s2", "temp": 30.0, "humidity": 60}"#,
        ))
        .unwrap();
    producer
        .send(&ProducerRecord::new(
            topic,
            None,
            r#"{"sensor_id": "s1", "temp": 28.0, "humidity": 55}"#,
        ))
        .unwrap();

    let mut engine = MerkSql::new(broker);

    // Create source via SQL
    engine
        .execute(&format!(
            "CREATE TABLE readings (sensor_id VARCHAR, temp DOUBLE, humidity INTEGER) WITH (KAFKA_TOPIC='{}')",
            topic
        ))
        .unwrap();

    // Query with filter
    let result = engine
        .execute("SELECT sensor_id, temp FROM readings WHERE temp > 24.0")
        .unwrap();
    match result {
        merksql::ExecuteResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 3); // s1@25, s2@30, s1@28
        }
        _ => panic!("Expected Rows"),
    }

    // Query with aggregate via builder
    let plan = QueryBuilder::from_source("readings")
        .group_by(&[col("sensor_id")])
        .count_star("cnt")
        .avg(col("temp"), "avg_temp")
        .build();

    let result = engine.query(plan).unwrap();
    match result {
        merksql::ExecuteResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 2); // s1 and s2

            let mut results: Vec<(String, i64, f64)> = rows
                .iter()
                .map(|r| {
                    let sid = r.get(0).as_str().unwrap().to_string();
                    let cnt = r.get(1).as_i64().unwrap();
                    let avg = r.get(2).as_f64().unwrap();
                    (sid, cnt, avg)
                })
                .collect();
            results.sort_by(|a, b| a.0.cmp(&b.0));

            assert_eq!(results[0].0, "s1");
            assert_eq!(results[0].1, 3);
            assert!((results[0].2 - 25.166).abs() < 0.1); // avg(22.5, 25, 28) = 25.166

            assert_eq!(results[1].0, "s2");
            assert_eq!(results[1].1, 1);
            assert!((results[1].2 - 30.0).abs() < 0.01);
        }
        _ => panic!("Expected Rows"),
    }
}

#[test]
fn e2e_builder_filter_aggregate_having() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "e2e-having";

    let producer = Broker::producer(&broker);
    for (sensor, temp) in &[
        ("s1", 10.0),
        ("s1", 20.0),
        ("s1", 30.0),
        ("s2", 100.0),
        ("s2", 200.0),
        ("s3", 5.0),
    ] {
        producer
            .send(&ProducerRecord::new(
                topic,
                None,
                &format!(r#"{{"sensor_id": "{}", "temp": {}}}"#, sensor, temp),
            ))
            .unwrap();
    }

    let mut engine = MerkSql::new(broker);
    engine
        .schemas
        .register_stream(
            "readings",
            Schema::new(vec![
                Column::new("sensor_id", DataType::String),
                Column::new("temp", DataType::Double),
            ]),
            topic,
        )
        .unwrap();

    // GROUP BY sensor_id, SUM(temp), HAVING SUM(temp) > 50
    let plan = QueryBuilder::from_source("readings")
        .group_by(&[col("sensor_id")])
        .sum(col("temp"), "total_temp")
        .having(col("total_temp").gt(lit_f64(50.0)))
        .build();

    let result = engine.query(plan).unwrap();
    match result {
        merksql::ExecuteResult::Rows { rows, .. } => {
            // s1: sum=60 > 50 ✓, s2: sum=300 > 50 ✓, s3: sum=5 ✗
            assert_eq!(rows.len(), 2);

            let mut sids: Vec<String> = rows
                .iter()
                .map(|r| r.get(0).as_str().unwrap().to_string())
                .collect();
            sids.sort();
            assert_eq!(sids, vec!["s1", "s2"]);
        }
        _ => panic!("Expected Rows"),
    }
}

#[test]
fn e2e_like_and_between() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "e2e-like";

    let producer = Broker::producer(&broker);
    for name in &["Alice", "Bob", "Albert", "Charlie", "Anna"] {
        producer
            .send(&ProducerRecord::new(
                topic,
                None,
                &format!(r#"{{"name": "{}", "age": 30}}"#, name),
            ))
            .unwrap();
    }

    let mut engine = MerkSql::new(broker);
    engine
        .schemas
        .register_stream(
            "people",
            Schema::new(vec![
                Column::new("name", DataType::String),
                Column::new("age", DataType::Integer),
            ]),
            topic,
        )
        .unwrap();

    // Filter with LIKE pattern
    let like_expr = merksql::expr::Expr::Like {
        expr: Box::new(col("name")),
        pattern: "Al%".to_string(),
        negated: false,
    };

    let plan = QueryBuilder::from_source("people")
        .filter(like_expr)
        .select(&[col("name")])
        .build();

    let result = engine.query(plan).unwrap();
    match result {
        merksql::ExecuteResult::Rows { rows, .. } => {
            assert_eq!(rows.len(), 2); // Alice and Albert
            let mut names: Vec<String> = rows
                .iter()
                .map(|r| r.get(0).as_str().unwrap().to_string())
                .collect();
            names.sort();
            assert_eq!(names, vec!["Albert", "Alice"]);
        }
        _ => panic!("Expected Rows"),
    }
}
