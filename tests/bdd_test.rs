use std::time::Duration;

use chrono::DateTime;
use merkql::broker::{Broker, BrokerConfig, BrokerRef};
use merkql::consumer::{ConsumerConfig, OffsetReset};
use merkql::record::ProducerRecord;
use tempfile::TempDir;

use merksql::builder::*;
use merksql::engine::pipeline;
use merksql::types::*;
use merksql::{ExecuteResult, MerkSql};

// === Shared Helpers ===

fn setup() -> (TempDir, BrokerRef, MerkSql) {
    let dir = tempfile::tempdir().unwrap();
    let broker = Broker::open(BrokerConfig::new(dir.path())).unwrap();
    let engine = MerkSql::new(broker.clone());
    (dir, broker, engine)
}

fn produce(broker: &BrokerRef, topic: &str, records: &[&str]) {
    let producer = Broker::producer(broker);
    for json in records {
        producer
            .send(&ProducerRecord::new(topic, None, *json))
            .unwrap();
    }
}

fn read_output(broker: &BrokerRef, topic: &str) -> Vec<String> {
    let mut consumer = Broker::consumer(
        broker,
        ConsumerConfig {
            group_id: format!("_bdd_reader_{}", uuid::Uuid::new_v4()),
            auto_commit: false,
            offset_reset: OffsetReset::Earliest,
        },
    );
    consumer.subscribe(&[topic]).unwrap();
    let records = consumer.poll(Duration::from_millis(200)).unwrap();
    consumer.close().unwrap();
    records.into_iter().map(|r| r.value).collect()
}

fn expect_rows(result: ExecuteResult) -> (Vec<Row>, Schema) {
    match result {
        ExecuteResult::Rows { rows, schema } => (rows, schema),
        other => panic!("Expected Rows, got {:?}", other),
    }
}

// === Scenario 1: Stream processing lifecycle ===

#[test]
fn scenario_1_stream_processing_lifecycle() {
    let (_dir, broker, mut engine) = setup();
    let topic = "bdd-events";

    produce(
        &broker,
        topic,
        &[
            r#"{"user_id": "u1", "action": "click", "value": 10}"#,
            r#"{"user_id": "u2", "action": "view", "value": 5}"#,
            r#"{"user_id": "u1", "action": "purchase", "value": 100}"#,
            r#"{"user_id": "u3", "action": "click", "value": 3}"#,
        ],
    );

    engine
        .execute(&format!(
            "CREATE TABLE events (user_id VARCHAR, action VARCHAR, value INTEGER) \
             WITH (KAFKA_TOPIC='{topic}')"
        ))
        .unwrap();

    let result = engine
        .execute("SELECT user_id, value FROM events WHERE action = 'click'")
        .unwrap();
    let (rows, schema) = expect_rows(result);

    assert_eq!(rows.len(), 2);
    assert_eq!(schema.columns.len(), 2);

    let mut user_ids: Vec<String> = rows
        .iter()
        .map(|r| r.get(0).as_str().unwrap().to_string())
        .collect();
    user_ids.sort();
    assert_eq!(user_ids, vec!["u1", "u3"]);
}

// === Scenario 2: Table aggregation ===

#[test]
fn scenario_2_table_aggregation() {
    let (_dir, broker, mut engine) = setup();
    let topic = "bdd-sales";

    produce(
        &broker,
        topic,
        &[
            r#"{"region": "east", "amount": 100}"#,
            r#"{"region": "west", "amount": 200}"#,
            r#"{"region": "east", "amount": 150}"#,
            r#"{"region": "west", "amount": 50}"#,
            r#"{"region": "east", "amount": 250}"#,
        ],
    );

    engine
        .execute(&format!(
            "CREATE TABLE sales (region VARCHAR, amount DOUBLE) \
             WITH (KAFKA_TOPIC='{topic}')"
        ))
        .unwrap();

    let plan = QueryBuilder::from_source("sales")
        .group_by(&[col("region")])
        .count_star("cnt")
        .sum(col("amount"), "total")
        .avg(col("amount"), "average")
        .build();

    let (rows, _schema) = expect_rows(engine.query(plan).unwrap());
    assert_eq!(rows.len(), 2);

    let mut results: Vec<(String, i64, f64, f64)> = rows
        .iter()
        .map(|r| {
            let region = r.get(0).as_str().unwrap().to_string();
            let cnt = r.get(1).as_i64().unwrap();
            let total = r.get(2).as_f64().unwrap();
            let avg = r.get(3).as_f64().unwrap();
            (region, cnt, total, avg)
        })
        .collect();
    results.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(results[0].0, "east");
    assert_eq!(results[0].1, 3);
    assert!((results[0].2 - 500.0).abs() < 0.01);
    assert!((results[0].3 - 166.666).abs() < 0.1);

    assert_eq!(results[1].0, "west");
    assert_eq!(results[1].1, 2);
    assert!((results[1].2 - 250.0).abs() < 0.01);
    assert!((results[1].3 - 125.0).abs() < 0.01);
}

// === Scenario 3: Windowed aggregation ===

#[test]
fn scenario_3_windowed_aggregation() {
    let (_dir, _broker, mut engine) = setup();

    engine
        .schemas
        .register_stream(
            "sensors",
            Schema::new(vec![
                Column::new("sensor_id", DataType::String),
                Column::new("temp", DataType::Double),
            ]),
            "bdd-sensors",
        )
        .unwrap();

    let plan = QueryBuilder::from_source("sensors")
        .group_by(&[col("sensor_id")])
        .tumbling(Duration::from_secs(60))
        .count_star("cnt")
        .avg(col("temp"), "avg_temp")
        .build();

    let mut compiled = pipeline::compile(&plan, &engine.schemas).unwrap();

    // Manually create timestamped rows in different windows
    let rows: Vec<Row> = [
        ("s1", 20.0, 10_000i64), // window [0, 60000)
        ("s1", 25.0, 30_000),    // window [0, 60000)
        ("s1", 30.0, 70_000),    // window [60000, 120000)
        ("s2", 15.0, 20_000),    // window [0, 60000)
    ]
    .iter()
    .map(|(sid, temp, ts)| {
        let json = format!(r#"{{"sensor_id": "{}", "temp": {}}}"#, sid, temp);
        Row::with_metadata(
            vec![Value::String(json)],
            RowMetadata {
                timestamp: DateTime::from_timestamp_millis(*ts),
                ..Default::default()
            },
        )
    })
    .collect();

    let result = compiled.process(rows).unwrap();

    // 3 groups: (s1, window0), (s1, window1), (s2, window0)
    assert_eq!(result.len(), 3);

    // All should have window metadata
    for row in &result {
        assert!(row.metadata.window_start.is_some());
        assert!(row.metadata.window_end.is_some());
    }

    // Check s1 in first window: cnt=2, avg=22.5
    let s1_w0: Vec<&Row> = result
        .iter()
        .filter(|r| {
            r.get(0).as_str().unwrap() == "s1"
                && r.metadata.window_start.unwrap().timestamp_millis() == 0
        })
        .collect();
    assert_eq!(s1_w0.len(), 1);
    assert_eq!(s1_w0[0].get(1).as_i64().unwrap(), 2);
    assert!((s1_w0[0].get(2).as_f64().unwrap() - 22.5).abs() < 0.01);
}

// === Scenario 4: Persistent query output ===

#[test]
fn scenario_4_persistent_query_output() {
    let (_dir, broker, mut engine) = setup();
    let topic = "bdd-input";

    produce(
        &broker,
        topic,
        &[
            r#"{"status": "active", "score": 10}"#,
            r#"{"status": "inactive", "score": 20}"#,
            r#"{"status": "active", "score": 30}"#,
            r#"{"status": "inactive", "score": 40}"#,
        ],
    );

    engine
        .schemas
        .register_stream(
            "items",
            Schema::new(vec![
                Column::new("status", DataType::String),
                Column::new("score", DataType::Integer),
            ]),
            topic,
        )
        .unwrap();

    // CTAS: filter active items into output topic
    let plan = QueryBuilder::from_source("items")
        .filter(col("status").eq_expr(lit_str("active")))
        .select(&[col("status"), col("score")])
        .as_stream("active_items", "bdd-output")
        .build();

    let result = engine.query(plan).unwrap();
    match &result {
        ExecuteResult::QueryStarted { id } => {
            assert!(!id.is_empty());
        }
        _ => panic!("Expected QueryStarted"),
    }

    // Wait for processing
    std::thread::sleep(Duration::from_millis(500));

    // Stop the query
    engine.queries.stop_all();

    // Read output topic
    let output = read_output(&broker, "bdd-output");
    assert_eq!(output.len(), 2);

    // Verify output is JSON objects with column names (Fix 1)
    for json_str in &output {
        let parsed: serde_json::Value = serde_json::from_str(json_str).unwrap();
        assert!(
            parsed.is_object(),
            "Expected JSON object, got: {}",
            json_str
        );
        let obj = parsed.as_object().unwrap();
        assert!(
            obj.contains_key("status"),
            "Missing 'status' key in: {}",
            json_str
        );
        assert!(
            obj.contains_key("score"),
            "Missing 'score' key in: {}",
            json_str
        );
        assert_eq!(obj["status"], "active");
    }

    // Verify scores
    let mut scores: Vec<i64> = output
        .iter()
        .map(|s| {
            let v: serde_json::Value = serde_json::from_str(s).unwrap();
            v["score"].as_i64().unwrap()
        })
        .collect();
    scores.sort();
    assert_eq!(scores, vec![10, 30]);
}

// === Scenario 5: Output is round-trippable ===

#[test]
fn scenario_5_output_is_round_trippable() {
    let (_dir, broker, mut engine) = setup();
    let input_topic = "bdd-round-in";

    produce(
        &broker,
        input_topic,
        &[
            r#"{"name": "Alice", "age": 30}"#,
            r#"{"name": "Bob", "age": 25}"#,
            r#"{"name": "Charlie", "age": 35}"#,
        ],
    );

    engine
        .schemas
        .register_stream(
            "people",
            Schema::new(vec![
                Column::new("name", DataType::String),
                Column::new("age", DataType::Integer),
            ]),
            input_topic,
        )
        .unwrap();

    // Persistent query: transform → output topic
    let plan = QueryBuilder::from_source("people")
        .select(&[col("name"), col("age")])
        .as_stream("people_out", "bdd-round-out")
        .build();

    engine.query(plan).unwrap();
    std::thread::sleep(Duration::from_millis(500));
    engine.queries.stop_all();

    // Now register the output topic as a new source
    engine
        .schemas
        .register_stream(
            "people_copy",
            Schema::new(vec![
                Column::new("name", DataType::String),
                Column::new("age", DataType::Integer),
            ]),
            "bdd-round-out",
        )
        .unwrap();

    // Query the copy — proves output format is valid input
    let plan2 = QueryBuilder::from_source("people_copy")
        .filter(col("age").gt(lit_i64(28)))
        .build();

    let (rows, _) = expect_rows(engine.query(plan2).unwrap());
    assert_eq!(rows.len(), 2); // Alice(30) and Charlie(35)

    let mut names: Vec<String> = rows
        .iter()
        .map(|r| r.get(0).as_str().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["Alice", "Charlie"]);
}

// === Scenario 6: Stream-table join ===

#[test]
fn scenario_6_stream_table_join() {
    let (_dir, broker, mut engine) = setup();

    // Customers table
    produce(
        &broker,
        "bdd-customers",
        &[
            r#"{"customer_id": "c1", "name": "Alice"}"#,
            r#"{"customer_id": "c2", "name": "Bob"}"#,
            r#"{"customer_id": "c3", "name": "Charlie"}"#,
        ],
    );

    // Orders stream
    produce(
        &broker,
        "bdd-orders",
        &[
            r#"{"order_id": "o1", "customer_id": "c1", "amount": 100}"#,
            r#"{"order_id": "o2", "customer_id": "c2", "amount": 200}"#,
            r#"{"order_id": "o3", "customer_id": "c1", "amount": 50}"#,
            r#"{"order_id": "o4", "customer_id": "c9", "amount": 999}"#,
        ],
    );

    engine
        .schemas
        .register_stream(
            "orders",
            Schema::new(vec![
                Column::new("order_id", DataType::String),
                Column::new("customer_id", DataType::String),
                Column::new("amount", DataType::Integer),
            ]),
            "bdd-orders",
        )
        .unwrap();

    engine
        .schemas
        .register_table(
            "customers",
            Schema::new(vec![
                Column::new("customer_id", DataType::String),
                Column::new("name", DataType::String),
            ]),
            "bdd-customers",
            "customer_id",
        )
        .unwrap();

    // Join orders with customers on customer_id (builder API)
    let plan = QueryBuilder::from_source("orders")
        .join(
            "customers",
            merksql::JoinType::Inner,
            col("customer_id").eq_expr(col("customer_id")),
        )
        .build();

    let (rows, schema) = expect_rows(engine.query(plan).unwrap());

    // c9 doesn't exist in customers → inner join should exclude it
    assert_eq!(rows.len(), 3);

    // Schema should have all columns from both sides
    assert_eq!(schema.columns.len(), 5); // order_id, customer_id, amount, customer_id, name

    // Verify enrichment: each row has a customer name
    for row in &rows {
        let name = row.get(4); // name is the last column from right side
        assert!(
            matches!(name, Value::String(_)),
            "Expected customer name, got {:?}",
            name
        );
    }

    // Verify specific joins
    let mut joined: Vec<(String, String)> = rows
        .iter()
        .map(|r| {
            let oid = r.get(0).as_str().unwrap().to_string();
            let name = r.get(4).as_str().unwrap().to_string();
            (oid, name)
        })
        .collect();
    joined.sort();
    assert_eq!(joined[0], ("o1".to_string(), "Alice".to_string()));
    assert_eq!(joined[1], ("o2".to_string(), "Bob".to_string()));
    assert_eq!(joined[2], ("o3".to_string(), "Alice".to_string()));
}

// === Scenario 7: SQL/builder parity ===

#[test]
fn scenario_7_sql_builder_parity() {
    let (_dir, broker, mut engine) = setup();
    let topic = "bdd-parity";

    produce(
        &broker,
        topic,
        &[
            r#"{"category": "A", "val": 10}"#,
            r#"{"category": "B", "val": 20}"#,
            r#"{"category": "A", "val": 30}"#,
            r#"{"category": "B", "val": 40}"#,
        ],
    );

    engine
        .execute(&format!(
            "CREATE TABLE data (category VARCHAR, val INTEGER) \
             WITH (KAFKA_TOPIC='{topic}')"
        ))
        .unwrap();

    // SQL query
    let (sql_rows, _) = expect_rows(
        engine
            .execute("SELECT category, val FROM data WHERE val > 15")
            .unwrap(),
    );

    // Builder query
    let plan = QueryBuilder::from_source("data")
        .filter(col("val").gt(lit_i64(15)))
        .select(&[col("category"), col("val")])
        .build();
    let (builder_rows, _) = expect_rows(engine.query(plan).unwrap());

    assert_eq!(sql_rows.len(), builder_rows.len());
    assert_eq!(sql_rows.len(), 3); // val=20, val=30, val=40

    // Both should return the same values
    let mut sql_vals: Vec<(String, i64)> = sql_rows
        .iter()
        .map(|r| {
            (
                r.get(0).as_str().unwrap().to_string(),
                r.get(1).as_i64().unwrap(),
            )
        })
        .collect();
    let mut builder_vals: Vec<(String, i64)> = builder_rows
        .iter()
        .map(|r| {
            (
                r.get(0).as_str().unwrap().to_string(),
                r.get(1).as_i64().unwrap(),
            )
        })
        .collect();
    sql_vals.sort();
    builder_vals.sort();
    assert_eq!(sql_vals, builder_vals);
}

// === Scenario 8: Multi-batch aggregation ===

#[test]
fn scenario_8_multi_batch_aggregation() {
    let (_dir, broker, mut engine) = setup();
    let topic = "bdd-multibatch";

    // Batch 1
    produce(
        &broker,
        topic,
        &[
            r#"{"dept": "eng", "salary": 100}"#,
            r#"{"dept": "sales", "salary": 80}"#,
            r#"{"dept": "eng", "salary": 120}"#,
        ],
    );

    engine
        .schemas
        .register_stream(
            "employees",
            Schema::new(vec![
                Column::new("dept", DataType::String),
                Column::new("salary", DataType::Double),
            ]),
            topic,
        )
        .unwrap();

    // Query after batch 1
    let plan = QueryBuilder::from_source("employees")
        .group_by(&[col("dept")])
        .count_star("cnt")
        .sum(col("salary"), "total")
        .build();
    let (rows1, _) = expect_rows(engine.query(plan).unwrap());

    let mut r1: Vec<(String, i64, f64)> = rows1
        .iter()
        .map(|r| {
            (
                r.get(0).as_str().unwrap().to_string(),
                r.get(1).as_i64().unwrap(),
                r.get(2).as_f64().unwrap(),
            )
        })
        .collect();
    r1.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(r1[0], ("eng".to_string(), 2, 220.0));
    assert_eq!(r1[1], ("sales".to_string(), 1, 80.0));

    // Batch 2
    produce(
        &broker,
        topic,
        &[
            r#"{"dept": "eng", "salary": 130}"#,
            r#"{"dept": "sales", "salary": 90}"#,
        ],
    );

    // Query after batch 2 — should see ALL data (pull query re-reads from beginning)
    let plan2 = QueryBuilder::from_source("employees")
        .group_by(&[col("dept")])
        .count_star("cnt")
        .sum(col("salary"), "total")
        .build();
    let (rows2, _) = expect_rows(engine.query(plan2).unwrap());

    let mut r2: Vec<(String, i64, f64)> = rows2
        .iter()
        .map(|r| {
            (
                r.get(0).as_str().unwrap().to_string(),
                r.get(1).as_i64().unwrap(),
                r.get(2).as_f64().unwrap(),
            )
        })
        .collect();
    r2.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(r2[0], ("eng".to_string(), 3, 350.0));
    assert_eq!(r2[1], ("sales".to_string(), 2, 170.0));
}

// === Scenario 9: Null handling ===

#[test]
fn scenario_9_null_handling() {
    let (_dir, broker, mut engine) = setup();
    let topic = "bdd-nulls";

    produce(
        &broker,
        topic,
        &[
            r#"{"name": "Alice", "score": 90}"#,
            r#"{"name": "Bob"}"#,
            r#"{"name": null, "score": 70}"#,
            r#"{"name": "Diana", "score": 85}"#,
        ],
    );

    engine
        .schemas
        .register_stream(
            "results",
            Schema::new(vec![
                Column::new("name", DataType::String),
                Column::new("score", DataType::Integer),
            ]),
            topic,
        )
        .unwrap();

    // Filter: score IS NOT NULL
    let plan = QueryBuilder::from_source("results")
        .filter(col("score").is_not_null())
        .build();
    let (rows, _) = expect_rows(engine.query(plan).unwrap());
    assert_eq!(rows.len(), 3); // Alice, null-name, Diana

    // COALESCE on name
    let coalesce = merksql::Expr::Function {
        name: "COALESCE".to_string(),
        args: vec![col("name"), lit_str("Unknown")],
    };
    let plan2 = QueryBuilder::from_source("results")
        .filter(col("score").is_not_null())
        .select(&[coalesce.alias("display_name"), col("score")])
        .build();
    let (rows2, _) = expect_rows(engine.query(plan2).unwrap());
    assert_eq!(rows2.len(), 3);

    let mut names: Vec<String> = rows2
        .iter()
        .map(|r| r.get(0).as_str().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["Alice", "Diana", "Unknown"]);

    // Aggregate with nulls: COUNT(score) should skip nulls, COUNT(*) should count all
    let plan3 = QueryBuilder::from_source("results")
        .group_by(&[])
        .count_star("total_rows")
        .count(col("score"), "scores_present")
        .build();
    let (rows3, _) = expect_rows(engine.query(plan3).unwrap());
    assert_eq!(rows3.len(), 1);
    assert_eq!(rows3[0].get(0).as_i64().unwrap(), 4); // COUNT(*)
    assert_eq!(rows3[0].get(1).as_i64().unwrap(), 3); // COUNT(score) — Bob has no score
}

// === Scenario 10: Complex expressions ===

#[test]
fn scenario_10_complex_expressions() {
    let (_dir, broker, mut engine) = setup();
    let topic = "bdd-expr";

    produce(
        &broker,
        topic,
        &[
            r#"{"product": "Widget A", "price": 15, "qty": 3}"#,
            r#"{"product": "Gadget Pro", "price": 150, "qty": 1}"#,
            r#"{"product": "Basic Widget", "price": 5, "qty": 10}"#,
            r#"{"product": "Premium Gadget", "price": 500, "qty": 2}"#,
        ],
    );

    engine
        .schemas
        .register_stream(
            "products",
            Schema::new(vec![
                Column::new("product", DataType::String),
                Column::new("price", DataType::Integer),
                Column::new("qty", DataType::Integer),
            ]),
            topic,
        )
        .unwrap();

    // CASE WHEN expression
    let tier = merksql::Expr::Case {
        operand: None,
        conditions: vec![
            (col("price").gt(lit_i64(100)), lit_str("premium")),
            (col("price").gt(lit_i64(10)), lit_str("standard")),
        ],
        else_result: Some(Box::new(lit_str("budget"))),
    };

    let plan = QueryBuilder::from_source("products")
        .select(&[col("product"), tier.alias("tier")])
        .build();
    let (rows, _) = expect_rows(engine.query(plan).unwrap());

    let mut results: Vec<(String, String)> = rows
        .iter()
        .map(|r| {
            (
                r.get(0).as_str().unwrap().to_string(),
                r.get(1).as_str().unwrap().to_string(),
            )
        })
        .collect();
    results.sort();
    assert_eq!(
        results[0],
        ("Basic Widget".to_string(), "budget".to_string())
    );
    assert_eq!(
        results[1],
        ("Gadget Pro".to_string(), "premium".to_string())
    );
    assert_eq!(
        results[2],
        ("Premium Gadget".to_string(), "premium".to_string())
    );
    assert_eq!(results[3], ("Widget A".to_string(), "standard".to_string()));

    // LIKE pattern
    let like = merksql::Expr::Like {
        expr: Box::new(col("product")),
        pattern: "%Widget%".to_string(),
        negated: false,
    };
    let plan2 = QueryBuilder::from_source("products")
        .filter(like)
        .select(&[col("product")])
        .build();
    let (rows2, _) = expect_rows(engine.query(plan2).unwrap());
    assert_eq!(rows2.len(), 2);

    let mut widget_names: Vec<String> = rows2
        .iter()
        .map(|r| r.get(0).as_str().unwrap().to_string())
        .collect();
    widget_names.sort();
    assert_eq!(widget_names, vec!["Basic Widget", "Widget A"]);

    // BETWEEN
    let between = merksql::Expr::Between {
        expr: Box::new(col("price")),
        low: Box::new(lit_i64(10)),
        high: Box::new(lit_i64(200)),
        negated: false,
    };
    let plan3 = QueryBuilder::from_source("products")
        .filter(between)
        .select(&[col("product"), col("price")])
        .build();
    let (rows3, _) = expect_rows(engine.query(plan3).unwrap());
    assert_eq!(rows3.len(), 2); // Widget A (15) and Gadget Pro (150)

    // CAST
    let cast_expr = merksql::Expr::Cast {
        expr: Box::new(col("price")),
        data_type: DataType::Double,
    };
    let plan4 = QueryBuilder::from_source("products")
        .select(&[col("product"), cast_expr.alias("price_d")])
        .build();
    let (rows4, _) = expect_rows(engine.query(plan4).unwrap());
    assert_eq!(rows4.len(), 4);
    // All price_d should be doubles
    for row in &rows4 {
        assert!(
            matches!(row.get(1), Value::Double(_)),
            "Expected Double, got {:?}",
            row.get(1)
        );
    }
}

// === Scenario 11: HAVING ===

#[test]
fn scenario_11_having() {
    let (_dir, broker, mut engine) = setup();
    let topic = "bdd-having";

    produce(
        &broker,
        topic,
        &[
            r#"{"city": "NYC", "temp": 30}"#,
            r#"{"city": "NYC", "temp": 32}"#,
            r#"{"city": "NYC", "temp": 28}"#,
            r#"{"city": "LA", "temp": 75}"#,
            r#"{"city": "LA", "temp": 80}"#,
            r#"{"city": "CHI", "temp": 10}"#,
        ],
    );

    engine
        .schemas
        .register_stream(
            "weather",
            Schema::new(vec![
                Column::new("city", DataType::String),
                Column::new("temp", DataType::Double),
            ]),
            topic,
        )
        .unwrap();

    // GROUP BY city, AVG(temp), HAVING AVG(temp) > 25
    let plan = QueryBuilder::from_source("weather")
        .group_by(&[col("city")])
        .avg(col("temp"), "avg_temp")
        .having(col("avg_temp").gt(lit_f64(25.0)))
        .build();

    let (rows, _) = expect_rows(engine.query(plan).unwrap());

    // NYC avg=30, LA avg=77.5, CHI avg=10 → only NYC and LA pass HAVING
    assert_eq!(rows.len(), 2);

    let mut cities: Vec<String> = rows
        .iter()
        .map(|r| r.get(0).as_str().unwrap().to_string())
        .collect();
    cities.sort();
    assert_eq!(cities, vec!["LA", "NYC"]);
}
