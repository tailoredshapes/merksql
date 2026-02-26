pub mod builder;
pub mod engine;
pub mod expr;
pub mod plan;
pub mod runtime;
pub mod schema;
pub mod sql;
pub mod types;

// Re-exports for convenience
pub use builder::{AggregateBuilder, JoinBuilder, QueryBuilder, SinkBuilder};
pub use engine::operator::Operator;
pub use engine::pipeline::{Pipeline, compile};
pub use expr::{
    BinaryOp, Expr, ExprExt, UnaryOp, col, lit_bool, lit_f64, lit_i64, lit_null, lit_str,
};
pub use plan::{AggregateExpr, AggregateFunction, JoinType, QueryPlan, SinkType, WindowSpec};
pub use runtime::persistent::QueryStatus;
pub use runtime::registry::QueryRegistry;
pub use schema::{SchemaRegistry, SourceInfo, SourceType};
pub use sql::parser::{SqlEngine, SqlResult};
pub use types::{Column, DataType, Row, RowMetadata, Schema, Value};

use anyhow::Result;
use merkql::broker::BrokerRef;

/// Top-level merksql engine providing both SQL and builder DSL interfaces.
pub struct MerkSql {
    pub broker: BrokerRef,
    pub schemas: SchemaRegistry,
    pub queries: QueryRegistry,
}

/// Result of executing a statement through the MerkSql engine.
#[derive(Debug)]
pub enum ExecuteResult {
    /// A source (stream/table) was created/registered.
    SourceCreated { name: String },
    /// A pull query returned rows.
    Rows { rows: Vec<Row>, schema: Schema },
    /// A persistent query was started.
    QueryStarted { id: String },
}

impl MerkSql {
    pub fn new(broker: BrokerRef) -> Self {
        Self {
            broker,
            schemas: SchemaRegistry::new(),
            queries: QueryRegistry::new(),
        }
    }

    /// Execute a SQL string (DDL or query).
    pub fn execute(&mut self, sql: &str) -> Result<ExecuteResult> {
        let result = SqlEngine::parse(sql, &mut self.schemas)?;
        match result {
            SqlResult::SourceCreated { name } => Ok(ExecuteResult::SourceCreated { name }),
            SqlResult::Query { plan } => self.execute_plan(plan),
        }
    }

    /// Execute a query plan (from builder DSL).
    pub fn query(&mut self, plan: QueryPlan) -> Result<ExecuteResult> {
        self.execute_plan(plan)
    }

    fn execute_plan(&mut self, plan: QueryPlan) -> Result<ExecuteResult> {
        // If the plan has a Sink, start a persistent query
        if let QueryPlan::Sink { ref topic, .. } = plan {
            let sink_topic = topic.clone();
            let id = self
                .queries
                .start_query(plan, sink_topic, &self.broker, &self.schemas)?;
            return Ok(ExecuteResult::QueryStarted { id });
        }

        // Otherwise, execute as a pull query
        let compiled = engine::pipeline::compile(&plan, &self.schemas)?;
        let output_schema = compiled.output_schema.clone();
        let rows = runtime::pull::pull_query(&self.broker, &plan, &self.schemas)?;
        Ok(ExecuteResult::Rows {
            rows,
            schema: output_schema,
        })
    }
}
