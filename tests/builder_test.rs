use std::time::Duration;

use merksql::builder::*;
use merksql::expr::Expr;
use merksql::plan::*;

#[test]
fn build_simple_scan() {
    let plan = QueryBuilder::from_source("readings").build();
    assert_eq!(
        plan,
        QueryPlan::Scan {
            source: "readings".to_string()
        }
    );
}

#[test]
fn build_filter() {
    let plan = QueryBuilder::from_source("readings")
        .filter(col("temp").gt(lit_f64(100.0)))
        .build();

    match &plan {
        QueryPlan::Filter { input, predicate } => {
            assert!(matches!(input.as_ref(), QueryPlan::Scan { source } if source == "readings"));
            assert!(matches!(predicate, Expr::BinaryOp { .. }));
        }
        _ => panic!("Expected Filter plan"),
    }
}

#[test]
fn build_filter_and_project() {
    let plan = QueryBuilder::from_source("readings")
        .filter(col("temp").gt(lit_f64(100.0)))
        .select(&[col("sensor_id"), col("temp")])
        .build();

    match &plan {
        QueryPlan::Project { input, expressions } => {
            assert_eq!(expressions.len(), 2);
            match input.as_ref() {
                QueryPlan::Filter { input, .. } => {
                    assert!(matches!(input.as_ref(), QueryPlan::Scan { .. }));
                }
                _ => panic!("Expected Filter inside Project"),
            }
        }
        _ => panic!("Expected Project plan"),
    }
}

#[test]
fn build_stream_sink() {
    let plan = QueryBuilder::from_source("readings")
        .filter(col("temp").gt(lit_f64(100.0)))
        .select(&[col("sensor_id"), col("temp")])
        .as_stream("high_temps", "high-temps-topic")
        .build();

    match &plan {
        QueryPlan::Sink {
            name,
            topic,
            sink_type,
            ..
        } => {
            assert_eq!(name, "high_temps");
            assert_eq!(topic, "high-temps-topic");
            assert_eq!(*sink_type, SinkType::Stream);
        }
        _ => panic!("Expected Sink plan"),
    }
}

#[test]
fn build_aggregate_count() {
    let plan = QueryBuilder::from_source("readings")
        .group_by(&[col("sensor_id")])
        .count_star("cnt")
        .build();

    match &plan {
        QueryPlan::Aggregate {
            group_by,
            aggregates,
            window,
            having,
            ..
        } => {
            assert_eq!(group_by.len(), 1);
            assert_eq!(aggregates.len(), 1);
            assert_eq!(aggregates[0].function, AggregateFunction::Count);
            assert_eq!(aggregates[0].alias, "cnt");
            assert!(window.is_none());
            assert!(having.is_none());
        }
        _ => panic!("Expected Aggregate plan"),
    }
}

#[test]
fn build_aggregate_with_multiple_functions() {
    let plan = QueryBuilder::from_source("readings")
        .group_by(&[col("sensor_id")])
        .count_star("cnt")
        .sum(col("temp"), "total_temp")
        .avg(col("temp"), "avg_temp")
        .min(col("temp"), "min_temp")
        .max(col("temp"), "max_temp")
        .build();

    match &plan {
        QueryPlan::Aggregate { aggregates, .. } => {
            assert_eq!(aggregates.len(), 5);
            assert_eq!(aggregates[0].function, AggregateFunction::Count);
            assert_eq!(aggregates[1].function, AggregateFunction::Sum);
            assert_eq!(aggregates[2].function, AggregateFunction::Avg);
            assert_eq!(aggregates[3].function, AggregateFunction::Min);
            assert_eq!(aggregates[4].function, AggregateFunction::Max);
        }
        _ => panic!("Expected Aggregate plan"),
    }
}

#[test]
fn build_tumbling_window() {
    let plan = QueryBuilder::from_source("readings")
        .group_by(&[col("sensor_id")])
        .tumbling(Duration::from_secs(300))
        .count_star("cnt")
        .build();

    match &plan {
        QueryPlan::Aggregate { window, .. } => match window.as_ref().unwrap() {
            WindowSpec::Tumbling { size, grace } => {
                assert_eq!(*size, Duration::from_secs(300));
                assert!(grace.is_none());
            }
            _ => panic!("Expected Tumbling window"),
        },
        _ => panic!("Expected Aggregate plan"),
    }
}

#[test]
fn build_hopping_window() {
    let plan = QueryBuilder::from_source("readings")
        .group_by(&[col("sensor_id")])
        .hopping(Duration::from_secs(300), Duration::from_secs(60))
        .count_star("cnt")
        .build();

    match &plan {
        QueryPlan::Aggregate { window, .. } => match window.as_ref().unwrap() {
            WindowSpec::Hopping { size, advance, .. } => {
                assert_eq!(*size, Duration::from_secs(300));
                assert_eq!(*advance, Duration::from_secs(60));
            }
            _ => panic!("Expected Hopping window"),
        },
        _ => panic!("Expected Aggregate plan"),
    }
}

#[test]
fn build_having() {
    let plan = QueryBuilder::from_source("readings")
        .group_by(&[col("sensor_id")])
        .count_star("cnt")
        .having(col("cnt").gt(lit_i64(10)))
        .build();

    match &plan {
        QueryPlan::Aggregate { having, .. } => {
            assert!(having.is_some());
        }
        _ => panic!("Expected Aggregate plan"),
    }
}

#[test]
fn build_aggregate_table_sink() {
    let plan = QueryBuilder::from_source("readings")
        .group_by(&[col("sensor_id")])
        .count_star("cnt")
        .as_table("sensor_counts", "sensor-counts-topic")
        .build();

    match &plan {
        QueryPlan::Sink { sink_type, .. } => {
            assert_eq!(*sink_type, SinkType::Table);
        }
        _ => panic!("Expected Sink plan"),
    }
}

#[test]
fn build_join() {
    let plan = QueryBuilder::from_source("orders")
        .join(
            "customers",
            JoinType::Left,
            col("orders.customer_id").eq_expr(col("customers.id")),
        )
        .build();

    match &plan {
        QueryPlan::Join {
            join_type, within, ..
        } => {
            assert_eq!(*join_type, JoinType::Left);
            assert!(within.is_none());
        }
        _ => panic!("Expected Join plan"),
    }
}

#[test]
fn build_join_within() {
    let plan = QueryBuilder::from_source("clicks")
        .join(
            "impressions",
            JoinType::Inner,
            col("clicks.ad_id").eq_expr(col("impressions.ad_id")),
        )
        .within(Duration::from_secs(3600))
        .build();

    match &plan {
        QueryPlan::Join { within, .. } => {
            assert_eq!(*within, Some(Duration::from_secs(3600)));
        }
        _ => panic!("Expected Join plan"),
    }
}

#[test]
fn plan_source_names() {
    let plan = QueryBuilder::from_source("readings")
        .filter(col("temp").gt(lit_f64(100.0)))
        .select(&[col("sensor_id")])
        .build();

    assert_eq!(plan.source_names(), vec!["readings"]);

    let plan = QueryBuilder::from_source("orders")
        .join("customers", JoinType::Inner, col("id").eq_expr(col("id")))
        .build();

    let sources = plan.source_names();
    assert_eq!(sources.len(), 2);
    assert!(sources.contains(&"orders".to_string()));
    assert!(sources.contains(&"customers".to_string()));
}
