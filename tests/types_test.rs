use merksql::types::*;
use std::collections::BTreeMap;

#[test]
fn value_null_checks() {
    assert!(Value::Null.is_null());
    assert!(!Value::Integer(0).is_null());
    assert!(!Value::String("".to_string()).is_null());
}

#[test]
fn value_truthiness() {
    assert!(!Value::Null.is_truthy());
    assert!(!Value::Boolean(false).is_truthy());
    assert!(Value::Boolean(true).is_truthy());
    assert!(!Value::Integer(0).is_truthy());
    assert!(Value::Integer(1).is_truthy());
    assert!(Value::Integer(-1).is_truthy());
    assert!(!Value::Double(0.0).is_truthy());
    assert!(Value::Double(1.0).is_truthy());
    assert!(!Value::String("".to_string()).is_truthy());
    assert!(Value::String("hello".to_string()).is_truthy());
}

#[test]
fn value_accessors() {
    assert_eq!(Value::Integer(42).as_i64(), Some(42));
    assert_eq!(Value::Double(3.14).as_f64(), Some(3.14));
    assert_eq!(Value::Integer(5).as_f64(), Some(5.0));
    assert_eq!(Value::Double(3.0).as_i64(), Some(3));
    assert_eq!(Value::String("hello".into()).as_str(), Some("hello"));
    assert_eq!(Value::Boolean(true).as_bool(), Some(true));
    assert_eq!(Value::Null.as_i64(), None);
    assert_eq!(Value::Null.as_str(), None);
}

#[test]
fn value_ordering() {
    // Null < everything
    assert!(Value::Null < Value::Integer(0));

    // Numeric comparison
    assert!(Value::Integer(1) < Value::Integer(2));
    assert!(Value::Double(1.0) < Value::Double(2.0));

    // String comparison
    assert!(Value::String("a".into()) < Value::String("b".into()));
}

#[test]
fn value_from_json() {
    let json: serde_json::Value = serde_json::json!({
        "name": "Alice",
        "age": 30,
        "score": 95.5,
        "active": true,
        "tags": ["rust", "sql"],
        "address": null
    });

    let value = Value::from(json);
    match &value {
        Value::Map(m) => {
            assert_eq!(m.get("name"), Some(&Value::String("Alice".to_string())));
            assert_eq!(m.get("age"), Some(&Value::Integer(30)));
            assert_eq!(m.get("score"), Some(&Value::Double(95.5)));
            assert_eq!(m.get("active"), Some(&Value::Boolean(true)));
            assert_eq!(m.get("address"), Some(&Value::Null));
            if let Some(Value::Array(tags)) = m.get("tags") {
                assert_eq!(tags.len(), 2);
            } else {
                panic!("Expected array for tags");
            }
        }
        _ => panic!("Expected map"),
    }
}

#[test]
fn value_to_json() {
    let value = Value::Map(BTreeMap::from([
        ("name".to_string(), Value::String("Bob".to_string())),
        ("age".to_string(), Value::Integer(25)),
    ]));

    let json: serde_json::Value = (&value).into();
    assert_eq!(json["name"], "Bob");
    assert_eq!(json["age"], 25);
}

#[test]
fn value_display() {
    assert_eq!(format!("{}", Value::Null), "NULL");
    assert_eq!(format!("{}", Value::Integer(42)), "42");
    assert_eq!(format!("{}", Value::Double(3.14)), "3.14");
    assert_eq!(format!("{}", Value::String("hello".into())), "hello");
    assert_eq!(format!("{}", Value::Boolean(true)), "true");
}

#[test]
fn schema_operations() {
    let schema = Schema::new(vec![
        Column::new("id", DataType::Integer),
        Column::new("name", DataType::String),
        Column::new("score", DataType::Double),
    ]);

    assert_eq!(schema.len(), 3);
    assert!(!schema.is_empty());

    // Case-insensitive lookup
    assert_eq!(schema.index_of("id"), Some(0));
    assert_eq!(schema.index_of("ID"), Some(0));
    assert_eq!(schema.index_of("Name"), Some(1));
    assert_eq!(schema.index_of("missing"), None);

    assert_eq!(schema.data_type("score"), Some(&DataType::Double));
    assert_eq!(schema.data_type("missing"), None);
}

#[test]
fn row_access() {
    let row = Row::new(vec![
        Value::Integer(1),
        Value::String("Alice".into()),
        Value::Double(95.5),
    ]);

    assert_eq!(row.get(0), &Value::Integer(1));
    assert_eq!(row.get(1), &Value::String("Alice".into()));
    // Out of bounds returns Null
    assert_eq!(row.get(10), &Value::Null);
}

#[test]
fn data_type_display() {
    assert_eq!(format!("{}", DataType::Integer), "INTEGER");
    assert_eq!(format!("{}", DataType::String), "STRING");
    assert_eq!(
        format!("{}", DataType::Array(Box::new(DataType::String))),
        "ARRAY<STRING>"
    );
    assert_eq!(
        format!(
            "{}",
            DataType::Map(Box::new(DataType::String), Box::new(DataType::Integer))
        ),
        "MAP<STRING, INTEGER>"
    );
}
