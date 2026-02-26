use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::{DataType, Row, Schema, Value};

/// Binary operators for expressions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinaryOp {
    // Comparison
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    // Logical
    And,
    Or,
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

/// Unary operators.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnaryOp {
    Not,
    Negate,
}

/// Expression AST for queries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    /// Column reference by name.
    Column(String),
    /// Literal value.
    Literal(Value),
    /// Binary operation: left op right.
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    /// Unary operation.
    UnaryOp { op: UnaryOp, expr: Box<Expr> },
    /// Function call: name(args...).
    Function { name: String, args: Vec<Expr> },
    /// IS NULL / IS NOT NULL.
    IsNull { expr: Box<Expr>, negated: bool },
    /// LIKE pattern matching.
    Like {
        expr: Box<Expr>,
        pattern: String,
        negated: bool,
    },
    /// BETWEEN low AND high.
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    /// CASE WHEN ... THEN ... ELSE ... END.
    Case {
        operand: Option<Box<Expr>>,
        conditions: Vec<(Expr, Expr)>,
        else_result: Option<Box<Expr>>,
    },
    /// CAST(expr AS type).
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
    },
    /// Alias: expr AS name.
    Alias { expr: Box<Expr>, name: String },
    /// Wildcard: SELECT *.
    Wildcard,
}

/// Evaluate an expression against a row and schema.
pub fn eval(expr: &Expr, row: &Row, schema: &Schema) -> Result<Value> {
    match expr {
        Expr::Column(name) => {
            // Check for metadata pseudo-columns
            match name.to_uppercase().as_str() {
                "ROWTIME" => Ok(row
                    .metadata
                    .timestamp
                    .map(Value::Timestamp)
                    .unwrap_or(Value::Null)),
                "ROWKEY" => Ok(row
                    .metadata
                    .key
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null)),
                "WINDOWSTART" => Ok(row
                    .metadata
                    .window_start
                    .map(Value::Timestamp)
                    .unwrap_or(Value::Null)),
                "WINDOWEND" => Ok(row
                    .metadata
                    .window_end
                    .map(Value::Timestamp)
                    .unwrap_or(Value::Null)),
                _ => match schema.index_of(name) {
                    Some(idx) => Ok(row.get(idx).clone()),
                    None => bail!("Unknown column: {}", name),
                },
            }
        }
        Expr::Literal(v) => Ok(v.clone()),
        Expr::BinaryOp { left, op, right } => {
            let lv = eval(left, row, schema)?;
            // Short-circuit for AND/OR
            match op {
                BinaryOp::And => {
                    if !lv.is_truthy() {
                        return Ok(Value::Boolean(false));
                    }
                    let rv = eval(right, row, schema)?;
                    return Ok(Value::Boolean(rv.is_truthy()));
                }
                BinaryOp::Or => {
                    if lv.is_truthy() {
                        return Ok(Value::Boolean(true));
                    }
                    let rv = eval(right, row, schema)?;
                    return Ok(Value::Boolean(rv.is_truthy()));
                }
                _ => {}
            }
            let rv = eval(right, row, schema)?;
            eval_binary_op(&lv, op, &rv)
        }
        Expr::UnaryOp { op, expr } => {
            let v = eval(expr, row, schema)?;
            eval_unary_op(op, &v)
        }
        Expr::Function { name, args } => {
            let evaluated: Vec<Value> = args
                .iter()
                .map(|a| eval(a, row, schema))
                .collect::<Result<_>>()?;
            eval_function(name, &evaluated)
        }
        Expr::IsNull { expr, negated } => {
            let v = eval(expr, row, schema)?;
            let is_null = v.is_null();
            Ok(Value::Boolean(if *negated { !is_null } else { is_null }))
        }
        Expr::Like {
            expr,
            pattern,
            negated,
        } => {
            let v = eval(expr, row, schema)?;
            match &v {
                Value::String(s) => {
                    let matches = like_match(s, pattern);
                    Ok(Value::Boolean(if *negated { !matches } else { matches }))
                }
                Value::Null => Ok(Value::Null),
                _ => bail!("LIKE requires a string, got {}", v.type_name()),
            }
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let v = eval(expr, row, schema)?;
            let lo = eval(low, row, schema)?;
            let hi = eval(high, row, schema)?;
            if v.is_null() || lo.is_null() || hi.is_null() {
                return Ok(Value::Null);
            }
            let in_range = v >= lo && v <= hi;
            Ok(Value::Boolean(if *negated { !in_range } else { in_range }))
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
        } => {
            if let Some(op_expr) = operand {
                let op_val = eval(op_expr, row, schema)?;
                for (when_expr, then_expr) in conditions {
                    let when_val = eval(when_expr, row, schema)?;
                    if op_val == when_val {
                        return eval(then_expr, row, schema);
                    }
                }
            } else {
                for (when_expr, then_expr) in conditions {
                    let when_val = eval(when_expr, row, schema)?;
                    if when_val.is_truthy() {
                        return eval(then_expr, row, schema);
                    }
                }
            }
            match else_result {
                Some(e) => eval(e, row, schema),
                None => Ok(Value::Null),
            }
        }
        Expr::Cast { expr, data_type } => {
            let v = eval(expr, row, schema)?;
            cast_value(&v, data_type)
        }
        Expr::Alias { expr, .. } => eval(expr, row, schema),
        Expr::Wildcard => bail!("Cannot evaluate wildcard directly"),
    }
}

fn eval_binary_op(left: &Value, op: &BinaryOp, right: &Value) -> Result<Value> {
    // NULL propagation for most operators
    if left.is_null() || right.is_null() {
        return match op {
            BinaryOp::Eq if left.is_null() && right.is_null() => Ok(Value::Boolean(true)),
            BinaryOp::NotEq if left.is_null() && right.is_null() => Ok(Value::Boolean(false)),
            _ => Ok(Value::Null),
        };
    }

    match op {
        BinaryOp::Eq => Ok(Value::Boolean(left == right)),
        BinaryOp::NotEq => Ok(Value::Boolean(left != right)),
        BinaryOp::Lt => Ok(Value::Boolean(left < right)),
        BinaryOp::LtEq => Ok(Value::Boolean(left <= right)),
        BinaryOp::Gt => Ok(Value::Boolean(left > right)),
        BinaryOp::GtEq => Ok(Value::Boolean(left >= right)),
        BinaryOp::And => Ok(Value::Boolean(left.is_truthy() && right.is_truthy())),
        BinaryOp::Or => Ok(Value::Boolean(left.is_truthy() || right.is_truthy())),
        BinaryOp::Add => numeric_op(left, right, |a, b| a + b, |a, b| a + b),
        BinaryOp::Sub => numeric_op(left, right, |a, b| a - b, |a, b| a - b),
        BinaryOp::Mul => numeric_op(left, right, |a, b| a * b, |a, b| a * b),
        BinaryOp::Div => {
            // Check for division by zero
            match right {
                Value::Integer(0) => bail!("Division by zero"),
                Value::Double(f) if *f == 0.0 => bail!("Division by zero"),
                _ => {}
            }
            numeric_op(left, right, |a, b| a / b, |a, b| a / b)
        }
        BinaryOp::Mod => {
            match right {
                Value::Integer(0) => bail!("Modulo by zero"),
                _ => {}
            }
            numeric_op(left, right, |a, b| a % b, |a, b| a % b)
        }
    }
}

fn numeric_op(
    left: &Value,
    right: &Value,
    int_op: impl Fn(i64, i64) -> i64,
    float_op: impl Fn(f64, f64) -> f64,
) -> Result<Value> {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => Ok(Value::Integer(int_op(*a, *b))),
        (Value::Double(a), Value::Double(b)) => Ok(Value::Double(float_op(*a, *b))),
        (Value::Integer(a), Value::Double(b)) => Ok(Value::Double(float_op(*a as f64, *b))),
        (Value::Double(a), Value::Integer(b)) => Ok(Value::Double(float_op(*a, *b as f64))),
        // String concatenation for Add
        (Value::String(a), Value::String(b)) => Ok(Value::String(format!("{a}{b}"))),
        _ => bail!(
            "Cannot apply arithmetic to {} and {}",
            left.type_name(),
            right.type_name()
        ),
    }
}

fn eval_unary_op(op: &UnaryOp, value: &Value) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    match op {
        UnaryOp::Not => Ok(Value::Boolean(!value.is_truthy())),
        UnaryOp::Negate => match value {
            Value::Integer(i) => Ok(Value::Integer(-i)),
            Value::Double(f) => Ok(Value::Double(-f)),
            _ => bail!("Cannot negate {}", value.type_name()),
        },
    }
}

fn eval_function(name: &str, args: &[Value]) -> Result<Value> {
    match name.to_uppercase().as_str() {
        "UPPER" | "UCASE" => {
            require_args(name, args, 1)?;
            match &args[0] {
                Value::String(s) => Ok(Value::String(s.to_uppercase())),
                Value::Null => Ok(Value::Null),
                _ => bail!("UPPER requires a string"),
            }
        }
        "LOWER" | "LCASE" => {
            require_args(name, args, 1)?;
            match &args[0] {
                Value::String(s) => Ok(Value::String(s.to_lowercase())),
                Value::Null => Ok(Value::Null),
                _ => bail!("LOWER requires a string"),
            }
        }
        "LEN" | "LENGTH" => {
            require_args(name, args, 1)?;
            match &args[0] {
                Value::String(s) => Ok(Value::Integer(s.len() as i64)),
                Value::Array(a) => Ok(Value::Integer(a.len() as i64)),
                Value::Null => Ok(Value::Null),
                _ => bail!("LEN requires a string or array"),
            }
        }
        "CONCAT" => {
            let mut result = String::new();
            for arg in args {
                match arg {
                    Value::String(s) => result.push_str(s),
                    Value::Null => return Ok(Value::Null),
                    other => result.push_str(&other.to_string()),
                }
            }
            Ok(Value::String(result))
        }
        "SUBSTRING" | "SUBSTR" => {
            if args.len() < 2 || args.len() > 3 {
                bail!("SUBSTRING requires 2 or 3 arguments");
            }
            match (&args[0], &args[1]) {
                (Value::String(s), Value::Integer(start)) => {
                    let start = (*start as usize).saturating_sub(1); // 1-based
                    let len = if args.len() == 3 {
                        args[2].as_i64().unwrap_or(s.len() as i64) as usize
                    } else {
                        s.len()
                    };
                    let result: String = s.chars().skip(start).take(len).collect();
                    Ok(Value::String(result))
                }
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                _ => bail!("SUBSTRING requires (string, int[, int])"),
            }
        }
        "ABS" => {
            require_args(name, args, 1)?;
            match &args[0] {
                Value::Integer(i) => Ok(Value::Integer(i.abs())),
                Value::Double(f) => Ok(Value::Double(f.abs())),
                Value::Null => Ok(Value::Null),
                _ => bail!("ABS requires a number"),
            }
        }
        "CEIL" | "CEILING" => {
            require_args(name, args, 1)?;
            match &args[0] {
                Value::Double(f) => Ok(Value::Integer(f.ceil() as i64)),
                Value::Integer(i) => Ok(Value::Integer(*i)),
                Value::Null => Ok(Value::Null),
                _ => bail!("CEIL requires a number"),
            }
        }
        "FLOOR" => {
            require_args(name, args, 1)?;
            match &args[0] {
                Value::Double(f) => Ok(Value::Integer(f.floor() as i64)),
                Value::Integer(i) => Ok(Value::Integer(*i)),
                Value::Null => Ok(Value::Null),
                _ => bail!("FLOOR requires a number"),
            }
        }
        "ROUND" => {
            require_args(name, args, 1)?;
            match &args[0] {
                Value::Double(f) => Ok(Value::Integer(f.round() as i64)),
                Value::Integer(i) => Ok(Value::Integer(*i)),
                Value::Null => Ok(Value::Null),
                _ => bail!("ROUND requires a number"),
            }
        }
        "COALESCE" => {
            for arg in args {
                if !arg.is_null() {
                    return Ok(arg.clone());
                }
            }
            Ok(Value::Null)
        }
        "IF" | "IIF" => {
            require_args(name, args, 3)?;
            if args[0].is_truthy() {
                Ok(args[1].clone())
            } else {
                Ok(args[2].clone())
            }
        }
        _ => bail!("Unknown function: {}", name),
    }
}

fn require_args(name: &str, args: &[Value], expected: usize) -> Result<()> {
    if args.len() != expected {
        bail!(
            "{} requires {} argument(s), got {}",
            name,
            expected,
            args.len()
        );
    }
    Ok(())
}

fn cast_value(value: &Value, target: &DataType) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    match target {
        DataType::Boolean => match value {
            Value::Boolean(_) => Ok(value.clone()),
            Value::String(s) => Ok(Value::Boolean(s.eq_ignore_ascii_case("true"))),
            Value::Integer(i) => Ok(Value::Boolean(*i != 0)),
            _ => bail!("Cannot cast {} to BOOLEAN", value.type_name()),
        },
        DataType::Integer | DataType::BigInt => match value {
            Value::Integer(_) => Ok(value.clone()),
            Value::Double(f) => Ok(Value::Integer(*f as i64)),
            Value::String(s) => {
                let i: i64 = s
                    .parse()
                    .map_err(|_| anyhow::anyhow!("Cannot cast '{}' to INTEGER", s))?;
                Ok(Value::Integer(i))
            }
            Value::Boolean(b) => Ok(Value::Integer(if *b { 1 } else { 0 })),
            _ => bail!("Cannot cast {} to INTEGER", value.type_name()),
        },
        DataType::Double => match value {
            Value::Double(_) => Ok(value.clone()),
            Value::Integer(i) => Ok(Value::Double(*i as f64)),
            Value::String(s) => {
                let f: f64 = s
                    .parse()
                    .map_err(|_| anyhow::anyhow!("Cannot cast '{}' to DOUBLE", s))?;
                Ok(Value::Double(f))
            }
            _ => bail!("Cannot cast {} to DOUBLE", value.type_name()),
        },
        DataType::String => Ok(Value::String(value.to_string())),
        DataType::Timestamp => match value {
            Value::Timestamp(_) => Ok(value.clone()),
            Value::String(s) => {
                let ts: DateTime<Utc> = s
                    .parse()
                    .map_err(|_| anyhow::anyhow!("Cannot cast '{}' to TIMESTAMP", s))?;
                Ok(Value::Timestamp(ts))
            }
            Value::Integer(millis) => {
                let ts = DateTime::from_timestamp_millis(*millis)
                    .ok_or_else(|| anyhow::anyhow!("Invalid timestamp millis: {}", millis))?;
                Ok(Value::Timestamp(ts))
            }
            _ => bail!("Cannot cast {} to TIMESTAMP", value.type_name()),
        },
        _ => bail!("Unsupported cast to {}", target),
    }
}

fn like_match(value: &str, pattern: &str) -> bool {
    // Convert SQL LIKE pattern to a simple matcher
    // % = any sequence, _ = any single char
    let mut regex = String::from("^");
    for ch in pattern.chars() {
        match ch {
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            '.' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '^' | '$' | '|' | '\\' => {
                regex.push('\\');
                regex.push(ch);
            }
            _ => regex.push(ch),
        }
    }
    regex.push('$');
    // Simple recursive matching instead of pulling in regex crate
    like_match_recursive(value.as_bytes(), pattern.as_bytes(), 0, 0)
}

fn like_match_recursive(value: &[u8], pattern: &[u8], vi: usize, pi: usize) -> bool {
    if pi == pattern.len() {
        return vi == value.len();
    }
    match pattern[pi] {
        b'%' => {
            // % matches any sequence (including empty)
            for i in vi..=value.len() {
                if like_match_recursive(value, pattern, i, pi + 1) {
                    return true;
                }
            }
            false
        }
        b'_' => {
            // _ matches exactly one character
            if vi < value.len() {
                like_match_recursive(value, pattern, vi + 1, pi + 1)
            } else {
                false
            }
        }
        ch => {
            if vi < value.len() && value[vi].eq_ignore_ascii_case(&ch) {
                like_match_recursive(value, pattern, vi + 1, pi + 1)
            } else {
                false
            }
        }
    }
}

// --- Expression builder helpers ---

/// Create a column reference expression.
pub fn col(name: &str) -> Expr {
    Expr::Column(name.to_string())
}

/// Create a string literal.
pub fn lit_str(s: &str) -> Expr {
    Expr::Literal(Value::String(s.to_string()))
}

/// Create an integer literal.
pub fn lit_i64(i: i64) -> Expr {
    Expr::Literal(Value::Integer(i))
}

/// Create a float literal.
pub fn lit_f64(f: f64) -> Expr {
    Expr::Literal(Value::Double(f))
}

/// Create a boolean literal.
pub fn lit_bool(b: bool) -> Expr {
    Expr::Literal(Value::Boolean(b))
}

/// Create a null literal.
pub fn lit_null() -> Expr {
    Expr::Literal(Value::Null)
}

/// Extension trait for building expressions fluently.
pub trait ExprExt {
    fn eq_expr(self, other: Expr) -> Expr;
    fn neq(self, other: Expr) -> Expr;
    fn lt(self, other: Expr) -> Expr;
    fn lt_eq(self, other: Expr) -> Expr;
    fn gt(self, other: Expr) -> Expr;
    fn gt_eq(self, other: Expr) -> Expr;
    fn and(self, other: Expr) -> Expr;
    fn or(self, other: Expr) -> Expr;
    fn add(self, other: Expr) -> Expr;
    fn sub(self, other: Expr) -> Expr;
    fn mul(self, other: Expr) -> Expr;
    fn div(self, other: Expr) -> Expr;
    fn modulo(self, other: Expr) -> Expr;
    fn alias(self, name: &str) -> Expr;
    fn is_null_expr(self) -> Expr;
    fn is_not_null(self) -> Expr;
}

impl ExprExt for Expr {
    fn eq_expr(self, other: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: BinaryOp::Eq,
            right: Box::new(other),
        }
    }
    fn neq(self, other: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: BinaryOp::NotEq,
            right: Box::new(other),
        }
    }
    fn lt(self, other: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: BinaryOp::Lt,
            right: Box::new(other),
        }
    }
    fn lt_eq(self, other: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: BinaryOp::LtEq,
            right: Box::new(other),
        }
    }
    fn gt(self, other: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: BinaryOp::Gt,
            right: Box::new(other),
        }
    }
    fn gt_eq(self, other: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: BinaryOp::GtEq,
            right: Box::new(other),
        }
    }
    fn and(self, other: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: BinaryOp::And,
            right: Box::new(other),
        }
    }
    fn or(self, other: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: BinaryOp::Or,
            right: Box::new(other),
        }
    }
    fn add(self, other: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: BinaryOp::Add,
            right: Box::new(other),
        }
    }
    fn sub(self, other: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: BinaryOp::Sub,
            right: Box::new(other),
        }
    }
    fn mul(self, other: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: BinaryOp::Mul,
            right: Box::new(other),
        }
    }
    fn div(self, other: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: BinaryOp::Div,
            right: Box::new(other),
        }
    }
    fn modulo(self, other: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(self),
            op: BinaryOp::Mod,
            right: Box::new(other),
        }
    }
    fn alias(self, name: &str) -> Expr {
        Expr::Alias {
            expr: Box::new(self),
            name: name.to_string(),
        }
    }
    fn is_null_expr(self) -> Expr {
        Expr::IsNull {
            expr: Box::new(self),
            negated: false,
        }
    }
    fn is_not_null(self) -> Expr {
        Expr::IsNull {
            expr: Box::new(self),
            negated: true,
        }
    }
}
