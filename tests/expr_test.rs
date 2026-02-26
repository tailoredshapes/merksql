use merksql::expr::*;
use merksql::types::*;

fn test_schema() -> Schema {
    Schema::new(vec![
        Column::new("name", DataType::String),
        Column::new("age", DataType::Integer),
        Column::new("score", DataType::Double),
        Column::new("active", DataType::Boolean),
    ])
}

fn test_row() -> Row {
    Row::new(vec![
        Value::String("Alice".to_string()),
        Value::Integer(30),
        Value::Double(95.5),
        Value::Boolean(true),
    ])
}

#[test]
fn eval_column_reference() {
    let schema = test_schema();
    let row = test_row();

    let result = eval(&col("name"), &row, &schema).unwrap();
    assert_eq!(result, Value::String("Alice".to_string()));

    let result = eval(&col("age"), &row, &schema).unwrap();
    assert_eq!(result, Value::Integer(30));
}

#[test]
fn eval_literal() {
    let schema = test_schema();
    let row = test_row();

    let result = eval(&lit_i64(42), &row, &schema).unwrap();
    assert_eq!(result, Value::Integer(42));

    let result = eval(&lit_str("hello"), &row, &schema).unwrap();
    assert_eq!(result, Value::String("hello".to_string()));
}

#[test]
fn eval_comparison() {
    let schema = test_schema();
    let row = test_row();

    // age > 25
    let expr = col("age").gt(lit_i64(25));
    assert_eq!(eval(&expr, &row, &schema).unwrap(), Value::Boolean(true));

    // age < 25
    let expr = col("age").lt(lit_i64(25));
    assert_eq!(eval(&expr, &row, &schema).unwrap(), Value::Boolean(false));

    // score >= 95.5
    let expr = col("score").gt_eq(lit_f64(95.5));
    assert_eq!(eval(&expr, &row, &schema).unwrap(), Value::Boolean(true));

    // name = 'Alice'
    let expr = col("name").eq_expr(lit_str("Alice"));
    assert_eq!(eval(&expr, &row, &schema).unwrap(), Value::Boolean(true));

    // name != 'Bob'
    let expr = col("name").neq(lit_str("Bob"));
    assert_eq!(eval(&expr, &row, &schema).unwrap(), Value::Boolean(true));
}

#[test]
fn eval_arithmetic() {
    let schema = test_schema();
    let row = test_row();

    // age + 10
    let expr = col("age").add(lit_i64(10));
    assert_eq!(eval(&expr, &row, &schema).unwrap(), Value::Integer(40));

    // score * 2.0
    let expr = col("score").mul(lit_f64(2.0));
    assert_eq!(eval(&expr, &row, &schema).unwrap(), Value::Double(191.0));

    // age - 5
    let expr = col("age").sub(lit_i64(5));
    assert_eq!(eval(&expr, &row, &schema).unwrap(), Value::Integer(25));

    // age / 3
    let expr = col("age").div(lit_i64(3));
    assert_eq!(eval(&expr, &row, &schema).unwrap(), Value::Integer(10));

    // age % 7
    let expr = col("age").modulo(lit_i64(7));
    assert_eq!(eval(&expr, &row, &schema).unwrap(), Value::Integer(2));
}

#[test]
fn eval_logical() {
    let schema = test_schema();
    let row = test_row();

    // active AND (age > 25)
    let expr = col("active").and(col("age").gt(lit_i64(25)));
    assert_eq!(eval(&expr, &row, &schema).unwrap(), Value::Boolean(true));

    // active AND (age > 35)
    let expr = col("active").and(col("age").gt(lit_i64(35)));
    assert_eq!(eval(&expr, &row, &schema).unwrap(), Value::Boolean(false));

    // (age < 25) OR active
    let expr = col("age").lt(lit_i64(25)).or(col("active"));
    assert_eq!(eval(&expr, &row, &schema).unwrap(), Value::Boolean(true));
}

#[test]
fn eval_is_null() {
    let schema = Schema::new(vec![Column::new("x", DataType::Integer)]);
    let row_null = Row::new(vec![Value::Null]);
    let row_val = Row::new(vec![Value::Integer(5)]);

    let expr = col("x").is_null_expr();
    assert_eq!(
        eval(&expr, &row_null, &schema).unwrap(),
        Value::Boolean(true)
    );
    assert_eq!(
        eval(&expr, &row_val, &schema).unwrap(),
        Value::Boolean(false)
    );

    let expr = col("x").is_not_null();
    assert_eq!(
        eval(&expr, &row_null, &schema).unwrap(),
        Value::Boolean(false)
    );
    assert_eq!(
        eval(&expr, &row_val, &schema).unwrap(),
        Value::Boolean(true)
    );
}

#[test]
fn eval_like() {
    let schema = test_schema();
    let row = test_row();

    // name LIKE 'Al%'
    let expr = Expr::Like {
        expr: Box::new(col("name")),
        pattern: "Al%".to_string(),
        negated: false,
    };
    assert_eq!(eval(&expr, &row, &schema).unwrap(), Value::Boolean(true));

    // name LIKE '%ice'
    let expr = Expr::Like {
        expr: Box::new(col("name")),
        pattern: "%ice".to_string(),
        negated: false,
    };
    assert_eq!(eval(&expr, &row, &schema).unwrap(), Value::Boolean(true));

    // name LIKE 'B%'
    let expr = Expr::Like {
        expr: Box::new(col("name")),
        pattern: "B%".to_string(),
        negated: false,
    };
    assert_eq!(eval(&expr, &row, &schema).unwrap(), Value::Boolean(false));

    // name LIKE 'A_ice'
    let expr = Expr::Like {
        expr: Box::new(col("name")),
        pattern: "A_ice".to_string(),
        negated: false,
    };
    assert_eq!(eval(&expr, &row, &schema).unwrap(), Value::Boolean(true));
}

#[test]
fn eval_between() {
    let schema = test_schema();
    let row = test_row();

    // age BETWEEN 25 AND 35
    let expr = Expr::Between {
        expr: Box::new(col("age")),
        low: Box::new(lit_i64(25)),
        high: Box::new(lit_i64(35)),
        negated: false,
    };
    assert_eq!(eval(&expr, &row, &schema).unwrap(), Value::Boolean(true));

    // age NOT BETWEEN 25 AND 29
    let expr = Expr::Between {
        expr: Box::new(col("age")),
        low: Box::new(lit_i64(25)),
        high: Box::new(lit_i64(29)),
        negated: true,
    };
    assert_eq!(eval(&expr, &row, &schema).unwrap(), Value::Boolean(true));
}

#[test]
fn eval_case() {
    let schema = test_schema();
    let row = test_row();

    // CASE WHEN age > 25 THEN 'adult' ELSE 'young' END
    let expr = Expr::Case {
        operand: None,
        conditions: vec![(col("age").gt(lit_i64(25)), lit_str("adult"))],
        else_result: Some(Box::new(lit_str("young"))),
    };
    assert_eq!(
        eval(&expr, &row, &schema).unwrap(),
        Value::String("adult".to_string())
    );

    // CASE name WHEN 'Alice' THEN 1 WHEN 'Bob' THEN 2 END
    let expr = Expr::Case {
        operand: Some(Box::new(col("name"))),
        conditions: vec![(lit_str("Alice"), lit_i64(1)), (lit_str("Bob"), lit_i64(2))],
        else_result: None,
    };
    assert_eq!(eval(&expr, &row, &schema).unwrap(), Value::Integer(1));
}

#[test]
fn eval_cast() {
    let schema = test_schema();
    let row = test_row();

    // CAST(age AS DOUBLE)
    let expr = Expr::Cast {
        expr: Box::new(col("age")),
        data_type: DataType::Double,
    };
    assert_eq!(eval(&expr, &row, &schema).unwrap(), Value::Double(30.0));

    // CAST(score AS INTEGER)
    let expr = Expr::Cast {
        expr: Box::new(col("score")),
        data_type: DataType::Integer,
    };
    assert_eq!(eval(&expr, &row, &schema).unwrap(), Value::Integer(95));

    // CAST(age AS STRING)
    let expr = Expr::Cast {
        expr: Box::new(col("age")),
        data_type: DataType::String,
    };
    assert_eq!(
        eval(&expr, &row, &schema).unwrap(),
        Value::String("30".to_string())
    );
}

#[test]
fn eval_functions() {
    let schema = test_schema();
    let row = test_row();

    // UPPER(name)
    let expr = Expr::Function {
        name: "UPPER".to_string(),
        args: vec![col("name")],
    };
    assert_eq!(
        eval(&expr, &row, &schema).unwrap(),
        Value::String("ALICE".to_string())
    );

    // LOWER(name)
    let expr = Expr::Function {
        name: "LOWER".to_string(),
        args: vec![col("name")],
    };
    assert_eq!(
        eval(&expr, &row, &schema).unwrap(),
        Value::String("alice".to_string())
    );

    // LEN(name)
    let expr = Expr::Function {
        name: "LEN".to_string(),
        args: vec![col("name")],
    };
    assert_eq!(eval(&expr, &row, &schema).unwrap(), Value::Integer(5));

    // ABS(-10)
    let expr = Expr::Function {
        name: "ABS".to_string(),
        args: vec![lit_i64(-10)],
    };
    assert_eq!(eval(&expr, &row, &schema).unwrap(), Value::Integer(10));

    // COALESCE(NULL, 'default')
    let expr = Expr::Function {
        name: "COALESCE".to_string(),
        args: vec![lit_null(), lit_str("default")],
    };
    assert_eq!(
        eval(&expr, &row, &schema).unwrap(),
        Value::String("default".to_string())
    );

    // CONCAT('Hello', ' ', 'World')
    let expr = Expr::Function {
        name: "CONCAT".to_string(),
        args: vec![lit_str("Hello"), lit_str(" "), lit_str("World")],
    };
    assert_eq!(
        eval(&expr, &row, &schema).unwrap(),
        Value::String("Hello World".to_string())
    );
}

#[test]
fn eval_alias() {
    let schema = test_schema();
    let row = test_row();

    // age AS years
    let expr = col("age").alias("years");
    assert_eq!(eval(&expr, &row, &schema).unwrap(), Value::Integer(30));
}

#[test]
fn eval_null_propagation() {
    let schema = Schema::new(vec![Column::new("x", DataType::Integer)]);
    let row = Row::new(vec![Value::Null]);

    // NULL + 5
    let expr = col("x").add(lit_i64(5));
    assert_eq!(eval(&expr, &row, &schema).unwrap(), Value::Null);

    // NULL > 5
    let expr = col("x").gt(lit_i64(5));
    assert_eq!(eval(&expr, &row, &schema).unwrap(), Value::Null);
}

#[test]
fn eval_unknown_column_error() {
    let schema = test_schema();
    let row = test_row();

    let result = eval(&col("nonexistent"), &row, &schema);
    assert!(result.is_err());
}

#[test]
fn eval_division_by_zero() {
    let schema = Schema::new(vec![Column::new("x", DataType::Integer)]);
    let row = Row::new(vec![Value::Integer(10)]);

    let expr = col("x").div(lit_i64(0));
    assert!(eval(&expr, &row, &schema).is_err());
}

#[test]
fn eval_metadata_columns() {
    let schema = Schema::new(vec![Column::new("x", DataType::Integer)]);
    let mut row = Row::new(vec![Value::Integer(1)]);
    row.metadata.key = Some("my-key".to_string());

    let result = eval(&col("ROWKEY"), &row, &schema).unwrap();
    assert_eq!(result, Value::String("my-key".to_string()));

    // No timestamp set → Null
    let result = eval(&col("ROWTIME"), &row, &schema).unwrap();
    assert_eq!(result, Value::Null);
}
