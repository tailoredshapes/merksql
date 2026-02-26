use merkql::broker::{Broker, BrokerConfig};
use merkql::record::ProducerRecord;

use merksql::expr::Expr;
use merksql::plan::*;
use merksql::runtime::pull;
use merksql::schema::SchemaRegistry;
use merksql::sql::parser::{SqlEngine, SqlResult};
use merksql::types::*;

fn setup_broker(dir: &tempfile::TempDir) -> merkql::broker::BrokerRef {
    Broker::open(BrokerConfig::new(dir.path())).unwrap()
}

// --- DDL Tests ---

#[test]
fn parse_create_stream() {
    let mut registry = SchemaRegistry::new();
    let sql = r#"
        CREATE TABLE readings (
            sensor_id VARCHAR,
            temp DOUBLE,
            humidity INT
        ) WITH (KAFKA_TOPIC='sensor-readings', VALUE_FORMAT='JSON')
    "#;

    let result = SqlEngine::parse(sql, &mut registry).unwrap();
    match result {
        SqlResult::SourceCreated { name } => {
            assert_eq!(name, "readings");
        }
        _ => panic!("Expected SourceCreated"),
    }

    let info = registry.get("readings").unwrap();
    assert_eq!(info.topic, "sensor-readings");
    assert_eq!(info.schema.len(), 3);
    assert_eq!(info.schema.columns[0].name, "sensor_id");
    assert_eq!(info.schema.columns[0].data_type, DataType::String);
    assert_eq!(info.schema.columns[1].name, "temp");
    assert_eq!(info.schema.columns[1].data_type, DataType::Double);
    assert_eq!(info.schema.columns[2].name, "humidity");
    assert_eq!(info.schema.columns[2].data_type, DataType::Integer);
}

#[test]
fn parse_create_table_with_key() {
    let mut registry = SchemaRegistry::new();
    let sql = r#"
        CREATE TABLE users (
            user_id VARCHAR,
            name VARCHAR,
            age INT
        ) WITH (KAFKA_TOPIC='users-topic', KEY='user_id')
    "#;

    let result = SqlEngine::parse(sql, &mut registry).unwrap();
    match result {
        SqlResult::SourceCreated { name } => {
            assert_eq!(name, "users");
        }
        _ => panic!("Expected SourceCreated"),
    }

    let info = registry.get("users").unwrap();
    assert_eq!(info.source_type, merksql::SourceType::Table);
    assert_eq!(info.key_column, Some("user_id".to_string()));
}

// --- SELECT Tests ---

#[test]
fn parse_select_star() {
    let mut registry = SchemaRegistry::new();
    register_readings(&mut registry);

    let sql = "SELECT * FROM readings";
    let result = SqlEngine::parse(sql, &mut registry).unwrap();

    match result {
        SqlResult::Query { plan } => {
            assert!(matches!(plan, QueryPlan::Scan { source } if source == "readings"));
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn parse_select_columns() {
    let mut registry = SchemaRegistry::new();
    register_readings(&mut registry);

    let sql = "SELECT sensor_id, temp FROM readings";
    let result = SqlEngine::parse(sql, &mut registry).unwrap();

    match result {
        SqlResult::Query { plan } => match &plan {
            QueryPlan::Project { expressions, .. } => {
                assert_eq!(expressions.len(), 2);
            }
            _ => panic!("Expected Project plan, got {:?}", plan),
        },
        _ => panic!("Expected Query"),
    }
}

#[test]
fn parse_select_with_alias() {
    let mut registry = SchemaRegistry::new();
    register_readings(&mut registry);

    let sql = "SELECT sensor_id, temp AS temperature FROM readings";
    let result = SqlEngine::parse(sql, &mut registry).unwrap();

    match result {
        SqlResult::Query { plan } => match &plan {
            QueryPlan::Project { expressions, .. } => {
                assert_eq!(expressions.len(), 2);
                assert!(
                    matches!(&expressions[1], Expr::Alias { name, .. } if name == "temperature")
                );
            }
            _ => panic!("Expected Project plan"),
        },
        _ => panic!("Expected Query"),
    }
}

#[test]
fn parse_select_where() {
    let mut registry = SchemaRegistry::new();
    register_readings(&mut registry);

    let sql = "SELECT * FROM readings WHERE temp > 100.0";
    let result = SqlEngine::parse(sql, &mut registry).unwrap();

    match result {
        SqlResult::Query { plan } => {
            assert!(matches!(plan, QueryPlan::Filter { .. }));
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn parse_select_where_and() {
    let mut registry = SchemaRegistry::new();
    register_readings(&mut registry);

    let sql = "SELECT sensor_id, temp FROM readings WHERE temp > 100.0 AND humidity < 80";
    let result = SqlEngine::parse(sql, &mut registry).unwrap();

    match result {
        SqlResult::Query { plan } => {
            // Project wrapping Filter wrapping Scan
            match &plan {
                QueryPlan::Project { input, .. } => {
                    assert!(matches!(input.as_ref(), QueryPlan::Filter { .. }));
                }
                _ => panic!("Expected Project(Filter(Scan))"),
            }
        }
        _ => panic!("Expected Query"),
    }
}

// --- GROUP BY Tests ---

#[test]
fn parse_group_by_count() {
    let mut registry = SchemaRegistry::new();
    register_readings(&mut registry);

    let sql = "SELECT sensor_id, COUNT(*) AS cnt FROM readings GROUP BY sensor_id";
    let result = SqlEngine::parse(sql, &mut registry).unwrap();

    match result {
        SqlResult::Query { plan } => match &plan {
            QueryPlan::Aggregate {
                group_by,
                aggregates,
                ..
            } => {
                assert_eq!(group_by.len(), 1);
                assert_eq!(aggregates.len(), 1);
                assert_eq!(aggregates[0].function, AggregateFunction::Count);
                assert_eq!(aggregates[0].alias, "cnt");
            }
            _ => panic!("Expected Aggregate plan"),
        },
        _ => panic!("Expected Query"),
    }
}

#[test]
fn parse_group_by_multiple_aggregates() {
    let mut registry = SchemaRegistry::new();
    register_readings(&mut registry);

    let sql = "SELECT sensor_id, COUNT(*) AS cnt, SUM(temp) AS total, AVG(temp) AS avg_temp FROM readings GROUP BY sensor_id";
    let result = SqlEngine::parse(sql, &mut registry).unwrap();

    match result {
        SqlResult::Query { plan } => match &plan {
            QueryPlan::Aggregate { aggregates, .. } => {
                assert_eq!(aggregates.len(), 3);
                assert_eq!(aggregates[0].function, AggregateFunction::Count);
                assert_eq!(aggregates[1].function, AggregateFunction::Sum);
                assert_eq!(aggregates[2].function, AggregateFunction::Avg);
            }
            _ => panic!("Expected Aggregate plan"),
        },
        _ => panic!("Expected Query"),
    }
}

#[test]
fn parse_having() {
    let mut registry = SchemaRegistry::new();
    register_readings(&mut registry);

    let sql =
        "SELECT sensor_id, COUNT(*) AS cnt FROM readings GROUP BY sensor_id HAVING COUNT(*) > 5";
    let result = SqlEngine::parse(sql, &mut registry).unwrap();

    match result {
        SqlResult::Query { plan } => match &plan {
            QueryPlan::Aggregate { having, .. } => {
                assert!(having.is_some());
            }
            _ => panic!("Expected Aggregate plan"),
        },
        _ => panic!("Expected Query"),
    }
}

// --- CREATE AS SELECT Tests ---

#[test]
fn parse_create_stream_as_select() {
    let mut registry = SchemaRegistry::new();
    register_readings(&mut registry);

    let sql = r#"
        CREATE TABLE high_temps
        WITH (KAFKA_TOPIC='high-temps')
        AS SELECT sensor_id, temp FROM readings WHERE temp > 100.0
    "#;
    let result = SqlEngine::parse(sql, &mut registry).unwrap();

    match result {
        SqlResult::Query { plan } => {
            match &plan {
                QueryPlan::Sink {
                    name,
                    topic,
                    sink_type,
                    input,
                } => {
                    assert_eq!(name, "high_temps");
                    assert_eq!(topic, "high-temps");
                    assert_eq!(*sink_type, SinkType::Stream);
                    // Input should be Project(Filter(Scan))
                    assert!(matches!(input.as_ref(), QueryPlan::Project { .. }));
                }
                _ => panic!("Expected Sink plan"),
            }
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn parse_create_table_as_select_aggregate() {
    let mut registry = SchemaRegistry::new();
    register_readings(&mut registry);

    let sql = r#"
        CREATE TABLE sensor_counts
        WITH (KAFKA_TOPIC='sensor-counts', KEY='sensor_id')
        AS SELECT sensor_id, COUNT(*) AS cnt FROM readings GROUP BY sensor_id
    "#;
    let result = SqlEngine::parse(sql, &mut registry).unwrap();

    match result {
        SqlResult::Query { plan } => match &plan {
            QueryPlan::Sink {
                name,
                sink_type,
                input,
                ..
            } => {
                assert_eq!(name, "sensor_counts");
                assert_eq!(*sink_type, SinkType::Table);
                assert!(matches!(input.as_ref(), QueryPlan::Aggregate { .. }));
            }
            _ => panic!("Expected Sink plan"),
        },
        _ => panic!("Expected Query"),
    }
}

// --- Expression Tests ---

#[test]
fn parse_expression_types() {
    let mut registry = SchemaRegistry::new();
    register_readings(&mut registry);

    // LIKE
    let sql = "SELECT * FROM readings WHERE sensor_id LIKE 's%'";
    let result = SqlEngine::parse(sql, &mut registry).unwrap();
    assert!(matches!(result, SqlResult::Query { .. }));

    // BETWEEN
    let sql = "SELECT * FROM readings WHERE temp BETWEEN 50 AND 100";
    let result = SqlEngine::parse(sql, &mut registry).unwrap();
    assert!(matches!(result, SqlResult::Query { .. }));

    // IS NULL
    let sql = "SELECT * FROM readings WHERE humidity IS NULL";
    let result = SqlEngine::parse(sql, &mut registry).unwrap();
    assert!(matches!(result, SqlResult::Query { .. }));

    // IS NOT NULL
    let sql = "SELECT * FROM readings WHERE humidity IS NOT NULL";
    let result = SqlEngine::parse(sql, &mut registry).unwrap();
    assert!(matches!(result, SqlResult::Query { .. }));
}

#[test]
fn parse_case_expression() {
    let mut registry = SchemaRegistry::new();
    register_readings(&mut registry);

    let sql = "SELECT sensor_id, CASE WHEN temp > 100 THEN 'hot' WHEN temp > 50 THEN 'warm' ELSE 'cold' END AS category FROM readings";
    let result = SqlEngine::parse(sql, &mut registry).unwrap();

    match result {
        SqlResult::Query { plan } => {
            assert!(matches!(plan, QueryPlan::Project { .. }));
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn parse_cast_expression() {
    let mut registry = SchemaRegistry::new();
    register_readings(&mut registry);

    let sql = "SELECT sensor_id, CAST(temp AS INT) AS temp_int FROM readings";
    let result = SqlEngine::parse(sql, &mut registry).unwrap();

    match result {
        SqlResult::Query { plan } => {
            assert!(matches!(plan, QueryPlan::Project { .. }));
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn parse_function_call() {
    let mut registry = SchemaRegistry::new();
    register_readings(&mut registry);

    let sql = "SELECT UPPER(sensor_id) AS sid FROM readings";
    let result = SqlEngine::parse(sql, &mut registry).unwrap();

    match result {
        SqlResult::Query { plan } => match &plan {
            QueryPlan::Project { expressions, .. } => match &expressions[0] {
                Expr::Alias { expr, name } => {
                    assert_eq!(name, "sid");
                    assert!(
                        matches!(expr.as_ref(), Expr::Function { name, .. } if name == "UPPER")
                    );
                }
                _ => panic!("Expected Alias(Function)"),
            },
            _ => panic!("Expected Project plan"),
        },
        _ => panic!("Expected Query"),
    }
}

// --- JOIN Tests ---

#[test]
fn parse_inner_join() {
    let mut registry = SchemaRegistry::new();
    register_readings(&mut registry);
    registry
        .register_stream(
            "sensors",
            Schema::new(vec![
                Column::new("id", DataType::String),
                Column::new("location", DataType::String),
            ]),
            "sensors-topic",
        )
        .unwrap();

    let sql = "SELECT * FROM readings INNER JOIN sensors ON readings.sensor_id = sensors.id";
    let result = SqlEngine::parse(sql, &mut registry).unwrap();

    match result {
        SqlResult::Query { plan } => match &plan {
            QueryPlan::Join { join_type, .. } => {
                assert_eq!(*join_type, JoinType::Inner);
            }
            _ => panic!("Expected Join plan"),
        },
        _ => panic!("Expected Query"),
    }
}

#[test]
fn parse_left_join() {
    let mut registry = SchemaRegistry::new();
    register_readings(&mut registry);
    registry
        .register_stream(
            "sensors",
            Schema::new(vec![
                Column::new("id", DataType::String),
                Column::new("location", DataType::String),
            ]),
            "sensors-topic",
        )
        .unwrap();

    let sql = "SELECT * FROM readings LEFT JOIN sensors ON readings.sensor_id = sensors.id";
    let result = SqlEngine::parse(sql, &mut registry).unwrap();

    match result {
        SqlResult::Query { plan } => match &plan {
            QueryPlan::Join { join_type, .. } => {
                assert_eq!(*join_type, JoinType::Left);
            }
            _ => panic!("Expected Join plan"),
        },
        _ => panic!("Expected Query"),
    }
}

// --- Integration: SQL to execution ---

#[test]
fn sql_end_to_end_filter() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "sensor-readings-e2e";

    // Register schema via SQL
    let mut registry = SchemaRegistry::new();
    let ddl = format!(
        "CREATE TABLE readings (sensor_id VARCHAR, temp DOUBLE, humidity INT) WITH (KAFKA_TOPIC='{}')",
        topic
    );
    SqlEngine::parse(&ddl, &mut registry).unwrap();

    // Produce data
    let producer = Broker::producer(&broker);
    for (sid, temp, hum) in [("s1", 22.5, 45), ("s2", 105.3, 80), ("s3", 110.0, 90)] {
        let json = format!(
            r#"{{"sensor_id": "{}", "temp": {}, "humidity": {}}}"#,
            sid, temp, hum
        );
        producer
            .send(&ProducerRecord::new(topic, None, json))
            .unwrap();
    }

    // Execute SQL via plan
    let sql = "SELECT sensor_id, temp FROM readings WHERE temp > 100.0";
    let result = SqlEngine::parse(sql, &mut registry).unwrap();

    match result {
        SqlResult::Query { plan } => {
            let rows = pull::pull_query(&broker, &plan, &registry).unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].get(0), &Value::String("s2".to_string()));
            assert_eq!(rows[1].get(0), &Value::String("s3".to_string()));
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn sql_end_to_end_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let broker = setup_broker(&dir);
    let topic = "orders-e2e";

    let mut registry = SchemaRegistry::new();
    let ddl = format!(
        "CREATE TABLE orders (customer_id VARCHAR, amount DOUBLE) WITH (KAFKA_TOPIC='{}')",
        topic
    );
    SqlEngine::parse(&ddl, &mut registry).unwrap();

    let producer = Broker::producer(&broker);
    for (cid, amount) in [
        ("c1", 10.0),
        ("c2", 25.0),
        ("c1", 15.0),
        ("c2", 30.0),
        ("c1", 5.0),
    ] {
        let json = format!(r#"{{"customer_id": "{}", "amount": {}}}"#, cid, amount);
        producer
            .send(&ProducerRecord::new(topic, None, json))
            .unwrap();
    }

    let sql = "SELECT customer_id, COUNT(*) AS cnt, SUM(amount) AS total FROM orders GROUP BY customer_id";
    let result = SqlEngine::parse(sql, &mut registry).unwrap();

    match result {
        SqlResult::Query { plan } => {
            let rows = pull::pull_query(&broker, &plan, &registry).unwrap();
            assert_eq!(rows.len(), 2);

            let mut results: Vec<(String, i64, f64)> = rows
                .iter()
                .map(|r| {
                    (
                        r.get(0).as_str().unwrap().to_string(),
                        r.get(1).as_i64().unwrap(),
                        r.get(2).as_f64().unwrap(),
                    )
                })
                .collect();
            results.sort_by(|a, b| a.0.cmp(&b.0));

            assert_eq!(results[0].0, "c1");
            assert_eq!(results[0].1, 3);
            assert!((results[0].2 - 30.0).abs() < 0.01);

            assert_eq!(results[1].0, "c2");
            assert_eq!(results[1].1, 2);
            assert!((results[1].2 - 55.0).abs() < 0.01);
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn parse_arithmetic_expression() {
    let mut registry = SchemaRegistry::new();
    register_readings(&mut registry);

    let sql = "SELECT sensor_id, temp * 1.8 + 32.0 AS temp_f FROM readings";
    let result = SqlEngine::parse(sql, &mut registry).unwrap();

    match result {
        SqlResult::Query { plan } => {
            assert!(matches!(plan, QueryPlan::Project { .. }));
        }
        _ => panic!("Expected Query"),
    }
}

#[test]
fn parse_error_invalid_sql() {
    let mut registry = SchemaRegistry::new();
    let result = SqlEngine::parse("NOT VALID SQL !!!", &mut registry);
    assert!(result.is_err());
}

// --- Helpers ---

fn register_readings(registry: &mut SchemaRegistry) {
    registry
        .register_stream(
            "readings",
            Schema::new(vec![
                Column::new("sensor_id", DataType::String),
                Column::new("temp", DataType::Double),
                Column::new("humidity", DataType::Integer),
            ]),
            "sensor-readings",
        )
        .unwrap();
}
