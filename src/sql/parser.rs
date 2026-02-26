use anyhow::{Result, bail};
use sqlparser::ast::{
    self, CaseWhen, CreateTableOptions, GroupByExpr, ObjectName, ObjectNamePart, Statement,
};
use sqlparser::parser::Parser;

use crate::expr::{BinaryOp, Expr, UnaryOp};
use crate::plan::{AggregateExpr, AggregateFunction, JoinType, QueryPlan, SinkType};
use crate::schema::SchemaRegistry;
use crate::sql::dialect::KsqlDialect;
use crate::types::{Column, DataType, Schema, Value};

/// Result of executing a SQL statement.
#[derive(Debug)]
pub enum SqlResult {
    /// A source was registered (CREATE STREAM/TABLE).
    SourceCreated { name: String },
    /// A query plan was produced (SELECT or CREATE ... AS SELECT).
    Query { plan: QueryPlan },
}

/// SQL engine: parses ksqlDB-style SQL into QueryPlan.
pub struct SqlEngine;

impl SqlEngine {
    /// Parse and translate a SQL string.
    pub fn parse(sql: &str, registry: &mut SchemaRegistry) -> Result<SqlResult> {
        let dialect = KsqlDialect;
        let statements = Parser::parse_sql(&dialect, sql)?;

        if statements.is_empty() {
            bail!("Empty SQL statement");
        }
        if statements.len() > 1 {
            bail!("Multiple statements not supported");
        }

        let stmt = &statements[0];
        match stmt {
            Statement::CreateTable(ct) => Self::handle_create_table(ct, registry),
            Statement::Query(query) => {
                let plan = Self::translate_query(query)?;
                Ok(SqlResult::Query { plan })
            }
            _ => bail!("Unsupported SQL statement: {}", stmt),
        }
    }

    fn handle_create_table(
        ct: &ast::CreateTable,
        registry: &mut SchemaRegistry,
    ) -> Result<SqlResult> {
        let name = object_name_to_string(&ct.name);

        // Extract WITH properties from table_options
        let props = Self::extract_with_properties(&ct.table_options);
        let is_table = props.iter().any(|(k, _)| k.eq_ignore_ascii_case("KEY"));

        // Check if this is a CTAS (CREATE ... AS SELECT ...)
        if let Some(query) = &ct.query {
            let input_plan = Self::translate_query(query)?;
            let topic = props
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("KAFKA_TOPIC"))
                .map(|(_, v)| v.clone())
                .unwrap_or_else(|| format!("{}_topic", name.to_lowercase()));

            let sink_type = if is_table {
                SinkType::Table
            } else {
                SinkType::Stream
            };

            let plan = QueryPlan::Sink {
                input: Box::new(input_plan),
                name: name.clone(),
                topic,
                sink_type,
            };
            return Ok(SqlResult::Query { plan });
        }

        // Regular CREATE STREAM/TABLE (DDL — register schema)
        let columns = Self::translate_columns(&ct.columns)?;
        let schema = Schema::new(columns);

        let topic = props
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("KAFKA_TOPIC"))
            .map(|(_, v)| v.clone())
            .ok_or_else(|| anyhow::anyhow!("Missing KAFKA_TOPIC in WITH clause"))?;

        if is_table {
            let key_col = props
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("KEY"))
                .map(|(_, v)| v.clone())
                .unwrap_or_else(|| {
                    schema
                        .columns
                        .first()
                        .map(|c| c.name.clone())
                        .unwrap_or_default()
                });
            registry.register_table(&name, schema, &topic, &key_col)?;
        } else {
            registry.register_stream(&name, schema, &topic)?;
        }

        Ok(SqlResult::SourceCreated { name })
    }

    fn extract_with_properties(table_options: &CreateTableOptions) -> Vec<(String, String)> {
        let options = match table_options {
            CreateTableOptions::With(opts) => opts,
            CreateTableOptions::Options(opts) => opts,
            CreateTableOptions::Plain(opts) => opts,
            CreateTableOptions::TableProperties(opts) => opts,
            CreateTableOptions::None => return vec![],
        };
        options
            .iter()
            .filter_map(|opt| match opt {
                ast::SqlOption::KeyValue { key, value } => {
                    let k = key.value.clone();
                    let v = match value {
                        ast::Expr::Value(v) => match &v.value {
                            ast::Value::SingleQuotedString(s) => s.clone(),
                            ast::Value::DoubleQuotedString(s) => s.clone(),
                            ast::Value::Number(n, _) => n.clone(),
                            other => other.to_string(),
                        },
                        _ => value.to_string(),
                    };
                    Some((k, v))
                }
                _ => None,
            })
            .collect()
    }

    fn translate_columns(columns: &[ast::ColumnDef]) -> Result<Vec<Column>> {
        columns
            .iter()
            .map(|col| {
                let name = col.name.value.clone();
                let data_type = Self::translate_data_type(&col.data_type)?;
                Ok(Column::new(name, data_type))
            })
            .collect()
    }

    fn translate_data_type(dt: &ast::DataType) -> Result<DataType> {
        match dt {
            ast::DataType::Boolean => Ok(DataType::Boolean),
            ast::DataType::Int(_) | ast::DataType::Integer(_) => Ok(DataType::Integer),
            ast::DataType::BigInt(_) => Ok(DataType::BigInt),
            ast::DataType::Float(_) | ast::DataType::Double(_) | ast::DataType::DoublePrecision => {
                Ok(DataType::Double)
            }
            ast::DataType::Varchar(_) | ast::DataType::Text | ast::DataType::String(_) => {
                Ok(DataType::String)
            }
            ast::DataType::Timestamp(_, _) => Ok(DataType::Timestamp),
            ast::DataType::Array(adt) => match adt {
                ast::ArrayElemTypeDef::AngleBracket(inner) => {
                    Ok(DataType::Array(Box::new(Self::translate_data_type(inner)?)))
                }
                ast::ArrayElemTypeDef::SquareBracket(inner, _) => {
                    Ok(DataType::Array(Box::new(Self::translate_data_type(inner)?)))
                }
                ast::ArrayElemTypeDef::Parenthesis(inner) => {
                    Ok(DataType::Array(Box::new(Self::translate_data_type(inner)?)))
                }
                ast::ArrayElemTypeDef::None => Ok(DataType::Array(Box::new(DataType::String))),
            },
            _ => bail!("Unsupported data type: {:?}", dt),
        }
    }

    fn translate_query(query: &ast::Query) -> Result<QueryPlan> {
        let body = query.body.as_ref();
        match body {
            ast::SetExpr::Select(select) => Self::translate_select(select),
            _ => bail!("Unsupported query type: {:?}", body),
        }
    }

    fn translate_select(select: &ast::Select) -> Result<QueryPlan> {
        // FROM clause
        if select.from.is_empty() {
            bail!("SELECT requires a FROM clause");
        }

        let mut plan = Self::translate_from(&select.from)?;

        // WHERE clause
        if let Some(selection) = &select.selection {
            let predicate = Self::translate_expr(selection)?;
            plan = QueryPlan::Filter {
                input: Box::new(plan),
                predicate,
            };
        }

        // GROUP BY clause
        let has_group_by = match &select.group_by {
            GroupByExpr::Expressions(exprs, _) => !exprs.is_empty(),
            GroupByExpr::All(_) => true,
        };

        if has_group_by {
            let group_exprs: Vec<Expr> = match &select.group_by {
                GroupByExpr::Expressions(exprs, _) => exprs
                    .iter()
                    .map(|e| Self::translate_expr(e))
                    .collect::<Result<_>>()?,
                GroupByExpr::All(_) => vec![],
            };

            let (_projections, aggregates) = Self::extract_aggregates(&select.projection)?;

            let having = select
                .having
                .as_ref()
                .map(|h| Self::translate_expr(h))
                .transpose()?;

            plan = QueryPlan::Aggregate {
                input: Box::new(plan),
                group_by: group_exprs,
                aggregates,
                window: None,
                having,
            };
        } else {
            // Regular SELECT projection
            let expressions = Self::translate_projection(&select.projection)?;
            if !Self::is_select_star(&expressions) {
                plan = QueryPlan::Project {
                    input: Box::new(plan),
                    expressions,
                };
            }
        }

        Ok(plan)
    }

    fn translate_from(from: &[ast::TableWithJoins]) -> Result<QueryPlan> {
        if from.len() > 1 {
            bail!("Multiple FROM tables not supported; use explicit JOIN");
        }

        let table = &from[0];
        let left = Self::translate_table_factor(&table.relation)?;

        if table.joins.is_empty() {
            return Ok(left);
        }

        // Handle joins
        let mut plan = left;
        for join in &table.joins {
            let right = Self::translate_table_factor(&join.relation)?;
            let (join_type, on_expr) = Self::translate_join_constraint(&join.join_operator)?;

            plan = QueryPlan::Join {
                left: Box::new(plan),
                right: Box::new(right),
                join_type,
                on: on_expr,
                within: None,
            };
        }

        Ok(plan)
    }

    fn translate_table_factor(factor: &ast::TableFactor) -> Result<QueryPlan> {
        match factor {
            ast::TableFactor::Table { name, .. } => {
                let source = object_name_to_string(name);
                Ok(QueryPlan::Scan { source })
            }
            _ => bail!("Unsupported FROM clause: {:?}", factor),
        }
    }

    fn translate_join_constraint(op: &ast::JoinOperator) -> Result<(JoinType, Expr)> {
        match op {
            ast::JoinOperator::Inner(constraint) => {
                let on = Self::extract_join_on(constraint)?;
                Ok((JoinType::Inner, on))
            }
            ast::JoinOperator::Left(constraint) | ast::JoinOperator::LeftOuter(constraint) => {
                let on = Self::extract_join_on(constraint)?;
                Ok((JoinType::Left, on))
            }
            ast::JoinOperator::Right(constraint) | ast::JoinOperator::RightOuter(constraint) => {
                let on = Self::extract_join_on(constraint)?;
                Ok((JoinType::Right, on))
            }
            ast::JoinOperator::FullOuter(constraint) => {
                let on = Self::extract_join_on(constraint)?;
                Ok((JoinType::FullOuter, on))
            }
            _ => bail!("Unsupported join type"),
        }
    }

    fn extract_join_on(constraint: &ast::JoinConstraint) -> Result<Expr> {
        match constraint {
            ast::JoinConstraint::On(expr) => Self::translate_expr(expr),
            _ => bail!("Only ON join constraint is supported"),
        }
    }

    fn translate_projection(projection: &[ast::SelectItem]) -> Result<Vec<Expr>> {
        let mut exprs = Vec::new();
        for item in projection {
            match item {
                ast::SelectItem::UnnamedExpr(e) => {
                    exprs.push(Self::translate_expr(e)?);
                }
                ast::SelectItem::ExprWithAlias { expr, alias } => {
                    let e = Self::translate_expr(expr)?;
                    exprs.push(Expr::Alias {
                        expr: Box::new(e),
                        name: alias.value.clone(),
                    });
                }
                ast::SelectItem::Wildcard(_) => {
                    exprs.push(Expr::Wildcard);
                }
                ast::SelectItem::QualifiedWildcard(_, _) => {
                    exprs.push(Expr::Wildcard);
                }
            }
        }
        Ok(exprs)
    }

    fn extract_aggregates(
        projection: &[ast::SelectItem],
    ) -> Result<(Vec<Expr>, Vec<AggregateExpr>)> {
        let mut projections = Vec::new();
        let mut aggregates = Vec::new();

        for item in projection {
            let (expr, alias) = match item {
                ast::SelectItem::UnnamedExpr(e) => (e, None),
                ast::SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.value.clone())),
                _ => continue,
            };

            if let Some(agg) = Self::try_extract_aggregate(expr, alias.as_deref())? {
                aggregates.push(agg);
            } else {
                projections.push(Self::translate_expr(expr)?);
            }
        }

        Ok((projections, aggregates))
    }

    fn try_extract_aggregate(
        expr: &ast::Expr,
        alias: Option<&str>,
    ) -> Result<Option<AggregateExpr>> {
        match expr {
            ast::Expr::Function(func) => {
                let func_name = object_name_to_string(&func.name).to_uppercase();
                let agg_fn = match func_name.as_str() {
                    "COUNT" => Some(AggregateFunction::Count),
                    "SUM" => Some(AggregateFunction::Sum),
                    "AVG" => Some(AggregateFunction::Avg),
                    "MIN" => Some(AggregateFunction::Min),
                    "MAX" => Some(AggregateFunction::Max),
                    "COLLECT_LIST" => Some(AggregateFunction::CollectList),
                    "COLLECT_SET" => Some(AggregateFunction::CollectSet),
                    _ => None,
                };

                if let Some(function) = agg_fn {
                    let args = match &func.args {
                        ast::FunctionArguments::List(arg_list) => &arg_list.args,
                        _ => bail!("Unsupported function argument format"),
                    };

                    let distinct = match &func.args {
                        ast::FunctionArguments::List(arg_list) => {
                            arg_list.duplicate_treatment == Some(ast::DuplicateTreatment::Distinct)
                        }
                        _ => false,
                    };

                    let agg_expr = if args.is_empty()
                        || matches!(
                            args.first(),
                            Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard))
                        ) {
                        Expr::Wildcard
                    } else {
                        match &args[0] {
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                                Self::translate_expr(e)?
                            }
                            _ => bail!("Unsupported aggregate argument"),
                        }
                    };

                    let default_alias = format!(
                        "{}_{}",
                        func_name.to_lowercase(),
                        if matches!(agg_expr, Expr::Wildcard) {
                            "star".to_string()
                        } else {
                            format!("{:?}", agg_expr)
                        }
                    );

                    Ok(Some(AggregateExpr {
                        function,
                        expr: agg_expr,
                        alias: alias.unwrap_or(&default_alias).to_string(),
                        distinct,
                    }))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None),
        }
    }

    fn translate_expr(expr: &ast::Expr) -> Result<Expr> {
        match expr {
            ast::Expr::Identifier(ident) => Ok(Expr::Column(ident.value.clone())),

            ast::Expr::CompoundIdentifier(parts) => {
                let name = parts
                    .iter()
                    .map(|p| p.value.as_str())
                    .collect::<Vec<_>>()
                    .join(".");
                Ok(Expr::Column(name))
            }

            ast::Expr::Value(v) => Self::translate_value(v),

            ast::Expr::BinaryOp { left, op, right } => {
                let l = Self::translate_expr(left)?;
                let r = Self::translate_expr(right)?;
                let bin_op = Self::translate_binary_op(op)?;
                Ok(Expr::BinaryOp {
                    left: Box::new(l),
                    op: bin_op,
                    right: Box::new(r),
                })
            }

            ast::Expr::UnaryOp { op, expr } => {
                let e = Self::translate_expr(expr)?;
                let unary_op = match op {
                    ast::UnaryOperator::Not => UnaryOp::Not,
                    ast::UnaryOperator::Minus => UnaryOp::Negate,
                    _ => bail!("Unsupported unary operator: {:?}", op),
                };
                Ok(Expr::UnaryOp {
                    op: unary_op,
                    expr: Box::new(e),
                })
            }

            ast::Expr::Function(func) => {
                let name = object_name_to_string(&func.name);
                let args = match &func.args {
                    ast::FunctionArguments::List(arg_list) => arg_list
                        .args
                        .iter()
                        .filter_map(|a| match a {
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => {
                                Some(Self::translate_expr(e))
                            }
                            _ => None,
                        })
                        .collect::<Result<Vec<_>>>()?,
                    ast::FunctionArguments::None => vec![],
                    _ => bail!("Unsupported function arguments"),
                };
                Ok(Expr::Function { name, args })
            }

            ast::Expr::IsNull(e) => {
                let inner = Self::translate_expr(e)?;
                Ok(Expr::IsNull {
                    expr: Box::new(inner),
                    negated: false,
                })
            }

            ast::Expr::IsNotNull(e) => {
                let inner = Self::translate_expr(e)?;
                Ok(Expr::IsNull {
                    expr: Box::new(inner),
                    negated: true,
                })
            }

            ast::Expr::Like {
                negated,
                expr,
                pattern,
                ..
            } => {
                let e = Self::translate_expr(expr)?;
                let pat = match pattern.as_ref() {
                    ast::Expr::Value(v) => match &v.value {
                        ast::Value::SingleQuotedString(s) => s.clone(),
                        ast::Value::DoubleQuotedString(s) => s.clone(),
                        _ => bail!("LIKE pattern must be a string"),
                    },
                    _ => bail!("LIKE pattern must be a string literal"),
                };
                Ok(Expr::Like {
                    expr: Box::new(e),
                    pattern: pat,
                    negated: *negated,
                })
            }

            ast::Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let e = Self::translate_expr(expr)?;
                let lo = Self::translate_expr(low)?;
                let hi = Self::translate_expr(high)?;
                Ok(Expr::Between {
                    expr: Box::new(e),
                    low: Box::new(lo),
                    high: Box::new(hi),
                    negated: *negated,
                })
            }

            ast::Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                let op = operand
                    .as_ref()
                    .map(|e| Self::translate_expr(e))
                    .transpose()?;
                let conds: Vec<(Expr, Expr)> = conditions
                    .iter()
                    .map(|cw: &CaseWhen| {
                        Ok((
                            Self::translate_expr(&cw.condition)?,
                            Self::translate_expr(&cw.result)?,
                        ))
                    })
                    .collect::<Result<_>>()?;
                let else_r = else_result
                    .as_ref()
                    .map(|e| Self::translate_expr(e))
                    .transpose()?;
                Ok(Expr::Case {
                    operand: op.map(Box::new),
                    conditions: conds,
                    else_result: else_r.map(Box::new),
                })
            }

            ast::Expr::Cast {
                expr, data_type, ..
            } => {
                let e = Self::translate_expr(expr)?;
                let dt = Self::translate_data_type(data_type)?;
                Ok(Expr::Cast {
                    expr: Box::new(e),
                    data_type: dt,
                })
            }

            ast::Expr::Nested(inner) => Self::translate_expr(inner),

            _ => bail!("Unsupported expression: {:?}", expr),
        }
    }

    fn translate_value(value: &ast::ValueWithSpan) -> Result<Expr> {
        match &value.value {
            ast::Value::Number(n, _) => {
                if n.contains('.') {
                    let f: f64 = n.parse()?;
                    Ok(Expr::Literal(Value::Double(f)))
                } else {
                    let i: i64 = n.parse()?;
                    Ok(Expr::Literal(Value::Integer(i)))
                }
            }
            ast::Value::SingleQuotedString(s) => Ok(Expr::Literal(Value::String(s.clone()))),
            ast::Value::DoubleQuotedString(s) => Ok(Expr::Literal(Value::String(s.clone()))),
            ast::Value::Boolean(b) => Ok(Expr::Literal(Value::Boolean(*b))),
            ast::Value::Null => Ok(Expr::Literal(Value::Null)),
            _ => bail!("Unsupported value: {:?}", value),
        }
    }

    fn translate_binary_op(op: &ast::BinaryOperator) -> Result<BinaryOp> {
        match op {
            ast::BinaryOperator::Eq => Ok(BinaryOp::Eq),
            ast::BinaryOperator::NotEq => Ok(BinaryOp::NotEq),
            ast::BinaryOperator::Lt => Ok(BinaryOp::Lt),
            ast::BinaryOperator::LtEq => Ok(BinaryOp::LtEq),
            ast::BinaryOperator::Gt => Ok(BinaryOp::Gt),
            ast::BinaryOperator::GtEq => Ok(BinaryOp::GtEq),
            ast::BinaryOperator::And => Ok(BinaryOp::And),
            ast::BinaryOperator::Or => Ok(BinaryOp::Or),
            ast::BinaryOperator::Plus => Ok(BinaryOp::Add),
            ast::BinaryOperator::Minus => Ok(BinaryOp::Sub),
            ast::BinaryOperator::Multiply => Ok(BinaryOp::Mul),
            ast::BinaryOperator::Divide => Ok(BinaryOp::Div),
            ast::BinaryOperator::Modulo => Ok(BinaryOp::Mod),
            _ => bail!("Unsupported binary operator: {:?}", op),
        }
    }

    fn is_select_star(expressions: &[Expr]) -> bool {
        expressions.len() == 1 && matches!(expressions[0], Expr::Wildcard)
    }
}

fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .filter_map(|p| match p {
            ObjectNamePart::Identifier(ident) => Some(ident.value.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(".")
}
