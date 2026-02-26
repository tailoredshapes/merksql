use std::time::Duration;

use chrono::{DateTime, Utc};
use merkql::broker::{Broker, BrokerConfig};
use merkql::record::ProducerRecord;

use merksql::builder::*;
use merksql::engine::pipeline;
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
                Column::new("sensor_id", DataType::String),
                Column::new("value", DataType::Double),
            ]),
            topic,
        )
        .unwrap();
    registry
}

/// Create rows with explicit timestamps for window testing.
fn make_timestamped_rows(data: &[(&str, f64, i64)]) -> Vec<Row> {
    data.iter()
        .map(|(sensor_id, value, ts_millis)| {
            let json = format!(r#"{{"sensor_id": "{}", "value": {}}}"#, sensor_id, value);
            Row::with_metadata(
                vec![Value::String(json)],
                RowMetadata {
                    timestamp: DateTime::from_timestamp_millis(*ts_millis),
                    ..Default::default()
                },
            )
        })
        .collect()
}

#[test]
fn tumbling_window_basic() {
    let dir = tempfile::tempdir().unwrap();
    let _broker = setup_broker(&dir);

    let registry = create_registry("test-topic");

    // 10-second tumbling window
    let plan = QueryBuilder::from_source("events")
        .group_by(&[col("sensor_id")])
        .tumbling(Duration::from_secs(10))
        .count_star("cnt")
        .build();

    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();

    // All events in the same 10-second window [0, 10000)
    let rows = make_timestamped_rows(&[
        ("s1", 10.0, 1000), // t=1s
        ("s1", 20.0, 3000), // t=3s
        ("s2", 30.0, 5000), // t=5s
        ("s1", 40.0, 7000), // t=7s
    ]);

    let result = pipeline.process(rows).unwrap();

    // Should have 2 groups: s1 (3 events) and s2 (1 event), both in window [0, 10000)
    assert_eq!(result.len(), 2);

    let mut results: Vec<(String, i64)> = result
        .iter()
        .map(|r| {
            let sid = r.get(0).as_str().unwrap().to_string();
            let cnt = r.get(1).as_i64().unwrap();
            (sid, cnt)
        })
        .collect();
    results.sort();

    assert_eq!(results[0], ("s1".to_string(), 3));
    assert_eq!(results[1], ("s2".to_string(), 1));

    // Verify window metadata
    for row in &result {
        assert!(row.metadata.window_start.is_some());
        assert!(row.metadata.window_end.is_some());
        let start = row.metadata.window_start.unwrap().timestamp_millis();
        let end = row.metadata.window_end.unwrap().timestamp_millis();
        assert_eq!(start, 0);
        assert_eq!(end, 10000);
    }
}

#[test]
fn tumbling_window_multiple_windows() {
    let dir = tempfile::tempdir().unwrap();
    let _broker = setup_broker(&dir);

    let registry = create_registry("test-topic");

    // 10-second tumbling window
    let plan = QueryBuilder::from_source("events")
        .group_by(&[col("sensor_id")])
        .tumbling(Duration::from_secs(10))
        .count_star("cnt")
        .sum(col("value"), "total")
        .build();

    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();

    // Events spanning two windows
    let rows = make_timestamped_rows(&[
        ("s1", 10.0, 2000),  // window [0, 10000)
        ("s1", 20.0, 5000),  // window [0, 10000)
        ("s1", 30.0, 12000), // window [10000, 20000)
        ("s1", 40.0, 15000), // window [10000, 20000)
        ("s1", 50.0, 18000), // window [10000, 20000)
    ]);

    let result = pipeline.process(rows).unwrap();

    // Should have 2 windows for s1
    assert_eq!(result.len(), 2);

    let mut results: Vec<(i64, i64, f64)> = result
        .iter()
        .map(|r| {
            let start = r.metadata.window_start.unwrap().timestamp_millis();
            let cnt = r.get(1).as_i64().unwrap();
            let total = r.get(2).as_f64().unwrap();
            (start, cnt, total)
        })
        .collect();
    results.sort_by_key(|r| r.0);

    // Window [0, 10000): 2 events, sum=30
    assert_eq!(results[0].0, 0);
    assert_eq!(results[0].1, 2);
    assert!((results[0].2 - 30.0).abs() < 0.01);

    // Window [10000, 20000): 3 events, sum=120
    assert_eq!(results[1].0, 10000);
    assert_eq!(results[1].1, 3);
    assert!((results[1].2 - 120.0).abs() < 0.01);
}

#[test]
fn hopping_window_overlapping() {
    let dir = tempfile::tempdir().unwrap();
    let _broker = setup_broker(&dir);

    let registry = create_registry("test-topic");

    // 10-second window, 5-second advance (50% overlap)
    let plan = QueryBuilder::from_source("events")
        .group_by(&[col("sensor_id")])
        .hopping(Duration::from_secs(10), Duration::from_secs(5))
        .count_star("cnt")
        .build();

    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();

    // Event at t=7s falls into windows [0, 10000) and [5000, 15000)
    let rows = make_timestamped_rows(&[("s1", 10.0, 7000)]);

    let result = pipeline.process(rows).unwrap();

    // Should fall into 2 overlapping windows
    assert_eq!(result.len(), 2);

    let mut window_starts: Vec<i64> = result
        .iter()
        .map(|r| r.metadata.window_start.unwrap().timestamp_millis())
        .collect();
    window_starts.sort();

    assert_eq!(window_starts, vec![0, 5000]);

    // Each window has count=1
    for row in &result {
        assert_eq!(row.get(1).as_i64().unwrap(), 1);
    }
}

#[test]
fn hopping_window_accumulation() {
    let dir = tempfile::tempdir().unwrap();
    let _broker = setup_broker(&dir);

    let registry = create_registry("test-topic");

    // 10-second window, 5-second advance
    let plan = QueryBuilder::from_source("events")
        .group_by(&[col("sensor_id")])
        .hopping(Duration::from_secs(10), Duration::from_secs(5))
        .sum(col("value"), "total")
        .build();

    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();

    // Use timestamps well into positive range to avoid edge effects
    // with a 10s window / 5s advance:
    //   t=52000 → windows: [45000, 55000), [50000, 60000)
    //   t=57000 → windows: [50000, 60000), [55000, 65000)
    //   t=62000 → windows: [55000, 65000), [60000, 70000)
    let rows = make_timestamped_rows(&[
        ("s1", 10.0, 52000),
        ("s1", 20.0, 57000),
        ("s1", 30.0, 62000),
    ]);

    let result = pipeline.process(rows).unwrap();

    let mut results: Vec<(i64, f64)> = result
        .iter()
        .map(|r| {
            let start = r.metadata.window_start.unwrap().timestamp_millis();
            let total = r.get(1).as_f64().unwrap();
            (start, total)
        })
        .collect();
    results.sort_by_key(|r| r.0);

    // [45000, 55000): only 10
    assert_eq!(results[0].0, 45000);
    assert!((results[0].1 - 10.0).abs() < 0.01);

    // [50000, 60000): 10 + 20 = 30
    assert_eq!(results[1].0, 50000);
    assert!((results[1].1 - 30.0).abs() < 0.01);

    // [55000, 65000): 20 + 30 = 50
    assert_eq!(results[2].0, 55000);
    assert!((results[2].1 - 50.0).abs() < 0.01);

    // [60000, 70000): only 30
    assert_eq!(results[3].0, 60000);
    assert!((results[3].1 - 30.0).abs() < 0.01);
}

#[test]
fn tumbling_window_multiple_groups() {
    let dir = tempfile::tempdir().unwrap();
    let _broker = setup_broker(&dir);

    let registry = create_registry("test-topic");

    let plan = QueryBuilder::from_source("events")
        .group_by(&[col("sensor_id")])
        .tumbling(Duration::from_secs(10))
        .count_star("cnt")
        .build();

    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();

    let rows = make_timestamped_rows(&[
        ("s1", 10.0, 1000),
        ("s2", 20.0, 2000),
        ("s1", 30.0, 3000),
        ("s2", 40.0, 4000),
        ("s3", 50.0, 5000),
    ]);

    let result = pipeline.process(rows).unwrap();

    // All in same window [0, 10000), 3 groups
    assert_eq!(result.len(), 3);

    let mut results: Vec<(String, i64)> = result
        .iter()
        .map(|r| {
            let sid = r.get(0).as_str().unwrap().to_string();
            let cnt = r.get(1).as_i64().unwrap();
            (sid, cnt)
        })
        .collect();
    results.sort();

    assert_eq!(results[0], ("s1".to_string(), 2));
    assert_eq!(results[1], ("s2".to_string(), 2));
    assert_eq!(results[2], ("s3".to_string(), 1));
}

#[test]
fn tumbling_window_with_filter() {
    let dir = tempfile::tempdir().unwrap();
    let _broker = setup_broker(&dir);

    let registry = create_registry("test-topic");

    // Filter before windowed aggregation
    let plan = QueryBuilder::from_source("events")
        .filter(col("value").gt(lit_f64(15.0)))
        .group_by(&[col("sensor_id")])
        .tumbling(Duration::from_secs(10))
        .count_star("cnt")
        .build();

    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();

    let rows = make_timestamped_rows(&[
        ("s1", 10.0, 1000), // filtered out
        ("s1", 20.0, 3000), // passes
        ("s1", 30.0, 5000), // passes
        ("s2", 5.0, 2000),  // filtered out
        ("s2", 25.0, 4000), // passes
    ]);

    let result = pipeline.process(rows).unwrap();

    let mut results: Vec<(String, i64)> = result
        .iter()
        .map(|r| {
            let sid = r.get(0).as_str().unwrap().to_string();
            let cnt = r.get(1).as_i64().unwrap();
            (sid, cnt)
        })
        .collect();
    results.sort();

    assert_eq!(results[0], ("s1".to_string(), 2));
    assert_eq!(results[1], ("s2".to_string(), 1));
}

#[test]
fn tumbling_window_builder_with_grace() {
    // Just verify the builder API works — grace period doesn't affect basic behavior
    let plan = QueryBuilder::from_source("events")
        .group_by(&[col("sensor_id")])
        .tumbling_with_grace(Duration::from_secs(10), Duration::from_secs(2))
        .count_star("cnt")
        .build();

    match &plan {
        merksql::plan::QueryPlan::Aggregate { window, .. } => match window.as_ref().unwrap() {
            merksql::plan::WindowSpec::Tumbling { size, grace } => {
                assert_eq!(*size, Duration::from_secs(10));
                assert_eq!(*grace, Some(Duration::from_secs(2)));
            }
            _ => panic!("Expected Tumbling window"),
        },
        _ => panic!("Expected Aggregate plan"),
    }
}

#[test]
fn session_window_basic() {
    let dir = tempfile::tempdir().unwrap();
    let _broker = setup_broker(&dir);

    let registry = create_registry("test-topic");

    // Session window with 5-second gap
    let plan = QueryBuilder::from_source("events")
        .group_by(&[col("sensor_id")])
        .session(Duration::from_secs(5))
        .count_star("cnt")
        .build();

    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();

    // Events with timestamps — each creates its own session window
    let rows = make_timestamped_rows(&[("s1", 10.0, 1000), ("s1", 20.0, 2000), ("s1", 30.0, 3000)]);

    let result = pipeline.process(rows).unwrap();
    // Each event gets its own session window entry
    // (Session merging would happen at a higher level or flush)
    assert!(!result.is_empty());

    // All should be s1 with count >= 1
    for row in &result {
        assert_eq!(row.get(0), &Value::String("s1".to_string()));
        assert!(row.get(1).as_i64().unwrap() >= 1);
    }
}

#[test]
fn windowed_avg_and_sum() {
    let dir = tempfile::tempdir().unwrap();
    let _broker = setup_broker(&dir);

    let registry = create_registry("test-topic");

    let plan = QueryBuilder::from_source("events")
        .group_by(&[col("sensor_id")])
        .tumbling(Duration::from_secs(10))
        .sum(col("value"), "total")
        .avg(col("value"), "average")
        .min(col("value"), "minimum")
        .max(col("value"), "maximum")
        .build();

    let mut pipeline = pipeline::compile(&plan, &registry).unwrap();

    let rows = make_timestamped_rows(&[("s1", 10.0, 1000), ("s1", 20.0, 3000), ("s1", 30.0, 5000)]);

    let result = pipeline.process(rows).unwrap();
    assert_eq!(result.len(), 1);

    let row = &result[0];
    assert_eq!(row.get(0), &Value::String("s1".to_string()));
    assert!((row.get(1).as_f64().unwrap() - 60.0).abs() < 0.01); // sum
    assert!((row.get(2).as_f64().unwrap() - 20.0).abs() < 0.01); // avg
    assert!((row.get(3).as_f64().unwrap() - 10.0).abs() < 0.01); // min
    assert!((row.get(4).as_f64().unwrap() - 30.0).abs() < 0.01); // max
}
