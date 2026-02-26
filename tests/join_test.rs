use std::time::Duration;

use chrono::DateTime;

use merksql::Operator;
use merksql::builder::*;
use merksql::engine::operator::{StreamStreamJoinOp, StreamTableJoinOp, TableTableJoinOp};
use merksql::plan::JoinType;
use merksql::types::*;

fn orders_schema() -> Schema {
    Schema::new(vec![
        Column::new("order_id", DataType::String),
        Column::new("customer_id", DataType::String),
        Column::new("amount", DataType::Double),
    ])
}

fn customers_schema() -> Schema {
    Schema::new(vec![
        Column::new("customer_id", DataType::String),
        Column::new("name", DataType::String),
    ])
}

fn make_row(values: Vec<Value>) -> Row {
    Row::new(values)
}

fn make_timestamped_row(values: Vec<Value>, ts_millis: i64) -> Row {
    Row::with_metadata(
        values,
        RowMetadata {
            timestamp: DateTime::from_timestamp_millis(ts_millis),
            ..Default::default()
        },
    )
}

// === Stream-Table Join Tests ===

#[test]
fn stream_table_inner_join() {
    let on = col("customer_id").eq_expr(col("customer_id"));

    let mut join = StreamTableJoinOp::new(JoinType::Inner, on, orders_schema(), customers_schema());

    // Load the table side (customers)
    join.load_right(
        vec![
            make_row(vec![
                Value::String("c1".into()),
                Value::String("Alice".into()),
            ]),
            make_row(vec![
                Value::String("c2".into()),
                Value::String("Bob".into()),
            ]),
        ],
        0, // key is customer_id at index 0
    );

    // Process stream side (orders)
    let orders = vec![
        make_row(vec![
            Value::String("o1".into()),
            Value::String("c1".into()),
            Value::Double(100.0),
        ]),
        make_row(vec![
            Value::String("o2".into()),
            Value::String("c2".into()),
            Value::Double(200.0),
        ]),
        make_row(vec![
            Value::String("o3".into()),
            Value::String("c3".into()), // no match
            Value::Double(300.0),
        ]),
    ];

    let result = join.process(orders).unwrap();

    // Inner join: only matching rows
    assert_eq!(result.len(), 2);

    // Combined row: [order_id, customer_id, amount, customer_id, name]
    // o1 joined with Alice
    assert_eq!(result[0].get(0), &Value::String("o1".into()));
    assert_eq!(result[0].get(1), &Value::String("c1".into()));
    assert_eq!(result[0].get(4), &Value::String("Alice".into()));

    // o2 joined with Bob
    assert_eq!(result[1].get(0), &Value::String("o2".into()));
    assert_eq!(result[1].get(4), &Value::String("Bob".into()));
}

#[test]
fn stream_table_left_join() {
    let on = col("customer_id").eq_expr(col("customer_id"));

    let mut join = StreamTableJoinOp::new(JoinType::Left, on, orders_schema(), customers_schema());

    join.load_right(
        vec![make_row(vec![
            Value::String("c1".into()),
            Value::String("Alice".into()),
        ])],
        0,
    );

    let orders = vec![
        make_row(vec![
            Value::String("o1".into()),
            Value::String("c1".into()),
            Value::Double(100.0),
        ]),
        make_row(vec![
            Value::String("o2".into()),
            Value::String("c999".into()), // no match
            Value::Double(200.0),
        ]),
    ];

    let result = join.process(orders).unwrap();

    // Left join: all left rows, nulls for unmatched right
    assert_eq!(result.len(), 2);

    // Combined row: [order_id, customer_id, amount, customer_id, name]
    // o1 matched
    assert_eq!(result[0].get(4), &Value::String("Alice".into()));

    // o2 unmatched — right side is NULL
    assert_eq!(result[1].get(0), &Value::String("o2".into()));
    assert_eq!(result[1].get(3), &Value::Null);
    assert_eq!(result[1].get(4), &Value::Null);
}

#[test]
fn stream_table_join_empty_table() {
    let on = col("customer_id").eq_expr(col("customer_id"));

    let mut join = StreamTableJoinOp::new(JoinType::Inner, on, orders_schema(), customers_schema());

    // Don't load any table data

    let orders = vec![make_row(vec![
        Value::String("o1".into()),
        Value::String("c1".into()),
        Value::Double(100.0),
    ])];

    let result = join.process(orders).unwrap();
    assert_eq!(result.len(), 0); // Inner join with empty table = no results
}

// === Stream-Stream Join Tests ===

#[test]
fn stream_stream_inner_join_within_window() {
    let on = col("customer_id").eq_expr(col("customer_id"));

    let mut join = StreamStreamJoinOp::new(
        JoinType::Inner,
        on,
        Duration::from_secs(10),
        orders_schema(),
        customers_schema(),
    );

    // First, add right-side rows
    let right_rows = vec![
        make_timestamped_row(
            vec![Value::String("c1".into()), Value::String("Alice".into())],
            5000,
        ),
        make_timestamped_row(
            vec![Value::String("c2".into()), Value::String("Bob".into())],
            6000,
        ),
    ];
    let _ = join.process_right(right_rows).unwrap();

    // Now process left-side rows within the window
    let left_rows = vec![
        make_timestamped_row(
            vec![
                Value::String("o1".into()),
                Value::String("c1".into()),
                Value::Double(100.0),
            ],
            8000, // within 10s of t=5000
        ),
        make_timestamped_row(
            vec![
                Value::String("o2".into()),
                Value::String("c2".into()),
                Value::Double(200.0),
            ],
            9000, // within 10s of t=6000
        ),
    ];

    let result = join.process_left(left_rows).unwrap();

    assert_eq!(result.len(), 2);
    // Combined row: [order_id, customer_id, amount, customer_id, name]
    // o1 joined with c1/Alice
    assert_eq!(result[0].get(0), &Value::String("o1".into()));
    assert_eq!(result[0].get(4), &Value::String("Alice".into()));
    // o2 joined with c2/Bob
    assert_eq!(result[1].get(0), &Value::String("o2".into()));
    assert_eq!(result[1].get(4), &Value::String("Bob".into()));
}

#[test]
fn stream_stream_join_outside_window() {
    let on = col("customer_id").eq_expr(col("customer_id"));

    let mut join = StreamStreamJoinOp::new(
        JoinType::Inner,
        on,
        Duration::from_secs(5), // 5-second window
        orders_schema(),
        customers_schema(),
    );

    // Right side at t=1000
    let right_rows = vec![make_timestamped_row(
        vec![Value::String("c1".into()), Value::String("Alice".into())],
        1000,
    )];
    let _ = join.process_right(right_rows).unwrap();

    // Left side at t=20000 — well outside the 5s window
    let left_rows = vec![make_timestamped_row(
        vec![
            Value::String("o1".into()),
            Value::String("c1".into()),
            Value::Double(100.0),
        ],
        20000,
    )];

    let result = join.process_left(left_rows).unwrap();
    assert_eq!(result.len(), 0); // No match — outside window
}

#[test]
fn stream_stream_left_join() {
    let on = col("customer_id").eq_expr(col("customer_id"));

    let mut join = StreamStreamJoinOp::new(
        JoinType::Left,
        on,
        Duration::from_secs(5),
        orders_schema(),
        customers_schema(),
    );

    // No right-side data

    let left_rows = vec![make_timestamped_row(
        vec![
            Value::String("o1".into()),
            Value::String("c1".into()),
            Value::Double(100.0),
        ],
        5000,
    )];

    let result = join.process_left(left_rows).unwrap();

    // Left join: unmatched left row emits with null right side
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get(0), &Value::String("o1".into()));
    assert_eq!(result[0].get(3), &Value::Null); // customer_id null
    assert_eq!(result[0].get(4), &Value::Null); // name null
}

#[test]
fn stream_stream_right_join() {
    let on = col("customer_id").eq_expr(col("customer_id"));

    let mut join = StreamStreamJoinOp::new(
        JoinType::Right,
        on,
        Duration::from_secs(5),
        orders_schema(),
        customers_schema(),
    );

    // Right side data, no matching left
    let right_rows = vec![make_timestamped_row(
        vec![Value::String("c1".into()), Value::String("Alice".into())],
        5000,
    )];

    let result = join.process_right(right_rows).unwrap();

    // Right join: unmatched right row emits with null left side
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get(0), &Value::Null); // order_id null
    assert_eq!(result[0].get(1), &Value::Null); // customer_id null
    assert_eq!(result[0].get(2), &Value::Null); // amount null
    assert_eq!(result[0].get(3), &Value::String("c1".into()));
    assert_eq!(result[0].get(4), &Value::String("Alice".into()));
}

// === Table-Table Join Tests ===

#[test]
fn table_table_inner_join() {
    let on = col("customer_id").eq_expr(col("customer_id"));

    let mut join = TableTableJoinOp::new(JoinType::Inner, on, orders_schema(), customers_schema());

    // Load right side
    join.load_right(
        vec![
            make_row(vec![
                Value::String("c1".into()),
                Value::String("Alice".into()),
            ]),
            make_row(vec![
                Value::String("c2".into()),
                Value::String("Bob".into()),
            ]),
        ],
        0,
    );

    // Load left side via process
    let orders = vec![
        make_row(vec![
            Value::String("o1".into()),
            Value::String("c1".into()),
            Value::Double(100.0),
        ]),
        make_row(vec![
            Value::String("o2".into()),
            Value::String("c2".into()),
            Value::Double(200.0),
        ]),
        make_row(vec![
            Value::String("o3".into()),
            Value::String("c3".into()), // no match
            Value::Double(300.0),
        ]),
    ];

    let result = join.process(orders).unwrap();

    // Inner join: only c1 and c2 match
    assert_eq!(result.len(), 2);
}

#[test]
fn table_table_left_join() {
    let on = col("customer_id").eq_expr(col("customer_id"));

    let mut join = TableTableJoinOp::new(JoinType::Left, on, orders_schema(), customers_schema());

    join.load_right(
        vec![make_row(vec![
            Value::String("c1".into()),
            Value::String("Alice".into()),
        ])],
        0,
    );

    let orders = vec![
        make_row(vec![
            Value::String("o1".into()),
            Value::String("c1".into()),
            Value::Double(100.0),
        ]),
        make_row(vec![
            Value::String("o2".into()),
            Value::String("c999".into()),
            Value::Double(200.0),
        ]),
    ];

    let result = join.process(orders).unwrap();

    // Left join: both rows, c999 has null right
    assert_eq!(result.len(), 2);

    let mut results: Vec<(String, Option<String>)> = result
        .iter()
        .map(|r| {
            let oid = r.get(0).as_str().unwrap().to_string();
            let name = match r.get(4) {
                Value::Null => None,
                Value::String(s) => Some(s.clone()),
                _ => None,
            };
            (oid, name)
        })
        .collect();
    results.sort();

    assert_eq!(results[0], ("o1".to_string(), Some("Alice".to_string())));
    assert_eq!(results[1], ("o2".to_string(), None));
}

#[test]
fn table_table_full_outer_join() {
    let on = col("customer_id").eq_expr(col("customer_id"));

    let mut join =
        TableTableJoinOp::new(JoinType::FullOuter, on, orders_schema(), customers_schema());

    // Right side has c1 and c3
    join.load_right(
        vec![
            make_row(vec![
                Value::String("c1".into()),
                Value::String("Alice".into()),
            ]),
            make_row(vec![
                Value::String("c3".into()),
                Value::String("Charlie".into()),
            ]),
        ],
        0,
    );

    // Left side has c1 and c2
    let orders = vec![
        make_row(vec![
            Value::String("o1".into()),
            Value::String("c1".into()),
            Value::Double(100.0),
        ]),
        make_row(vec![
            Value::String("o2".into()),
            Value::String("c2".into()),
            Value::Double(200.0),
        ]),
    ];

    let result = join.process(orders).unwrap();

    // Full outer: c1 matched, c2 left-only (null right), c3 right-only (null left)
    assert_eq!(result.len(), 3);

    // Check that we have all expected patterns
    let mut has_matched = false;
    let mut has_left_only = false;
    let mut has_right_only = false;

    for row in &result {
        let left_null = row.get(0) == &Value::Null;
        let right_null = row.get(3) == &Value::Null;

        if !left_null && !right_null {
            has_matched = true;
        } else if !left_null && right_null {
            has_left_only = true;
        } else if left_null && !right_null {
            has_right_only = true;
        }
    }

    assert!(has_matched, "Should have matched row (c1)");
    assert!(has_left_only, "Should have left-only row (c2)");
    assert!(has_right_only, "Should have right-only row (c3)");
}

#[test]
fn table_table_right_join() {
    let on = col("customer_id").eq_expr(col("customer_id"));

    let mut join = TableTableJoinOp::new(JoinType::Right, on, orders_schema(), customers_schema());

    // Right side
    join.load_right(
        vec![
            make_row(vec![
                Value::String("c1".into()),
                Value::String("Alice".into()),
            ]),
            make_row(vec![
                Value::String("c2".into()),
                Value::String("Bob".into()),
            ]),
        ],
        0,
    );

    // Left side only has c1
    let orders = vec![make_row(vec![
        Value::String("o1".into()),
        Value::String("c1".into()),
        Value::Double(100.0),
    ])];

    let result = join.process(orders).unwrap();

    // Right join: c1 matched + c2 right-only
    assert_eq!(result.len(), 2);
}

// === Builder API Join Test ===

#[test]
fn builder_join_plan() {
    let plan = QueryBuilder::from_source("orders")
        .join(
            "customers",
            JoinType::Inner,
            col("customer_id").eq_expr(col("customer_id")),
        )
        .build();

    match &plan {
        merksql::plan::QueryPlan::Join {
            join_type, within, ..
        } => {
            assert_eq!(*join_type, JoinType::Inner);
            assert!(within.is_none());
        }
        _ => panic!("Expected Join plan"),
    }
}

#[test]
fn builder_join_with_within() {
    let plan = QueryBuilder::from_source("orders")
        .join(
            "customers",
            JoinType::Left,
            col("customer_id").eq_expr(col("customer_id")),
        )
        .within(Duration::from_secs(30))
        .build();

    match &plan {
        merksql::plan::QueryPlan::Join {
            join_type, within, ..
        } => {
            assert_eq!(*join_type, JoinType::Left);
            assert_eq!(*within, Some(Duration::from_secs(30)));
        }
        _ => panic!("Expected Join plan"),
    }
}

#[test]
fn builder_join_with_select() {
    let plan = QueryBuilder::from_source("orders")
        .join(
            "customers",
            JoinType::Inner,
            col("customer_id").eq_expr(col("customer_id")),
        )
        .select(&[col("order_id"), col("name")])
        .build();

    match &plan {
        merksql::plan::QueryPlan::Project { input, .. } => {
            assert!(matches!(**input, merksql::plan::QueryPlan::Join { .. }));
        }
        _ => panic!("Expected Project wrapping Join"),
    }
}
