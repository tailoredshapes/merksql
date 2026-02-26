use std::time::Duration;

use crate::expr::Expr;
use crate::plan::{AggregateExpr, AggregateFunction, JoinType, QueryPlan, SinkType, WindowSpec};

/// Builder for constructing query plans fluently.
pub struct QueryBuilder {
    plan: QueryPlan,
}

impl QueryBuilder {
    /// Start building a query from a named source.
    pub fn from_source(name: &str) -> Self {
        Self {
            plan: QueryPlan::Scan {
                source: name.to_string(),
            },
        }
    }

    /// Add a filter predicate (WHERE clause).
    pub fn filter(self, predicate: Expr) -> Self {
        Self {
            plan: QueryPlan::Filter {
                input: Box::new(self.plan),
                predicate,
            },
        }
    }

    /// Add a projection (SELECT clause).
    pub fn select(self, expressions: &[Expr]) -> Self {
        Self {
            plan: QueryPlan::Project {
                input: Box::new(self.plan),
                expressions: expressions.to_vec(),
            },
        }
    }

    /// Start building an aggregation with GROUP BY.
    pub fn group_by(self, keys: &[Expr]) -> AggregateBuilder {
        AggregateBuilder {
            input: self.plan,
            group_by: keys.to_vec(),
            aggregates: vec![],
            window: None,
            having: None,
        }
    }

    /// Join with another source.
    pub fn join(self, right_source: &str, join_type: JoinType, on: Expr) -> JoinBuilder {
        JoinBuilder {
            left: self.plan,
            right: QueryPlan::Scan {
                source: right_source.to_string(),
            },
            join_type,
            on,
            within: None,
        }
    }

    /// Write output as a stream to a topic.
    pub fn as_stream(self, name: &str, topic: &str) -> SinkBuilder {
        SinkBuilder {
            input: self.plan,
            name: name.to_string(),
            topic: topic.to_string(),
            sink_type: SinkType::Stream,
        }
    }

    /// Write output as a table to a topic.
    pub fn as_table(self, name: &str, topic: &str) -> SinkBuilder {
        SinkBuilder {
            input: self.plan,
            name: name.to_string(),
            topic: topic.to_string(),
            sink_type: SinkType::Table,
        }
    }

    /// Build the query plan (without a sink).
    pub fn build(self) -> QueryPlan {
        self.plan
    }
}

/// Builder for aggregate queries.
pub struct AggregateBuilder {
    input: QueryPlan,
    group_by: Vec<Expr>,
    aggregates: Vec<AggregateExpr>,
    window: Option<WindowSpec>,
    having: Option<Expr>,
}

impl AggregateBuilder {
    /// Add COUNT(*) aggregate.
    pub fn count_star(mut self, alias: &str) -> Self {
        self.aggregates.push(AggregateExpr {
            function: AggregateFunction::Count,
            expr: Expr::Wildcard,
            alias: alias.to_string(),
            distinct: false,
        });
        self
    }

    /// Add COUNT(expr) aggregate.
    pub fn count(mut self, expr: Expr, alias: &str) -> Self {
        self.aggregates.push(AggregateExpr {
            function: AggregateFunction::Count,
            expr,
            alias: alias.to_string(),
            distinct: false,
        });
        self
    }

    /// Add COUNT(DISTINCT expr) aggregate.
    pub fn count_distinct(mut self, expr: Expr, alias: &str) -> Self {
        self.aggregates.push(AggregateExpr {
            function: AggregateFunction::Count,
            expr,
            alias: alias.to_string(),
            distinct: true,
        });
        self
    }

    /// Add SUM(expr) aggregate.
    pub fn sum(mut self, expr: Expr, alias: &str) -> Self {
        self.aggregates.push(AggregateExpr {
            function: AggregateFunction::Sum,
            expr,
            alias: alias.to_string(),
            distinct: false,
        });
        self
    }

    /// Add AVG(expr) aggregate.
    pub fn avg(mut self, expr: Expr, alias: &str) -> Self {
        self.aggregates.push(AggregateExpr {
            function: AggregateFunction::Avg,
            expr,
            alias: alias.to_string(),
            distinct: false,
        });
        self
    }

    /// Add MIN(expr) aggregate.
    pub fn min(mut self, expr: Expr, alias: &str) -> Self {
        self.aggregates.push(AggregateExpr {
            function: AggregateFunction::Min,
            expr,
            alias: alias.to_string(),
            distinct: false,
        });
        self
    }

    /// Add MAX(expr) aggregate.
    pub fn max(mut self, expr: Expr, alias: &str) -> Self {
        self.aggregates.push(AggregateExpr {
            function: AggregateFunction::Max,
            expr,
            alias: alias.to_string(),
            distinct: false,
        });
        self
    }

    /// Add COLLECT_LIST(expr) aggregate.
    pub fn collect_list(mut self, expr: Expr, alias: &str) -> Self {
        self.aggregates.push(AggregateExpr {
            function: AggregateFunction::CollectList,
            expr,
            alias: alias.to_string(),
            distinct: false,
        });
        self
    }

    /// Add COLLECT_SET(expr) aggregate.
    pub fn collect_set(mut self, expr: Expr, alias: &str) -> Self {
        self.aggregates.push(AggregateExpr {
            function: AggregateFunction::CollectSet,
            expr,
            alias: alias.to_string(),
            distinct: false,
        });
        self
    }

    /// Add TOPK(k, expr) aggregate.
    pub fn topk(mut self, k: usize, expr: Expr, alias: &str) -> Self {
        self.aggregates.push(AggregateExpr {
            function: AggregateFunction::TopK(k),
            expr,
            alias: alias.to_string(),
            distinct: false,
        });
        self
    }

    /// Add a generic aggregate expression.
    pub fn aggregate(mut self, agg: AggregateExpr) -> Self {
        self.aggregates.push(agg);
        self
    }

    /// Set a tumbling window.
    pub fn tumbling(mut self, size: Duration) -> Self {
        self.window = Some(WindowSpec::Tumbling { size, grace: None });
        self
    }

    /// Set a tumbling window with grace period.
    pub fn tumbling_with_grace(mut self, size: Duration, grace: Duration) -> Self {
        self.window = Some(WindowSpec::Tumbling {
            size,
            grace: Some(grace),
        });
        self
    }

    /// Set a hopping window.
    pub fn hopping(mut self, size: Duration, advance: Duration) -> Self {
        self.window = Some(WindowSpec::Hopping {
            size,
            advance,
            grace: None,
        });
        self
    }

    /// Set a hopping window with grace period.
    pub fn hopping_with_grace(
        mut self,
        size: Duration,
        advance: Duration,
        grace: Duration,
    ) -> Self {
        self.window = Some(WindowSpec::Hopping {
            size,
            advance,
            grace: Some(grace),
        });
        self
    }

    /// Set a session window.
    pub fn session(mut self, gap: Duration) -> Self {
        self.window = Some(WindowSpec::Session { gap, grace: None });
        self
    }

    /// Set a session window with grace period.
    pub fn session_with_grace(mut self, gap: Duration, grace: Duration) -> Self {
        self.window = Some(WindowSpec::Session {
            gap,
            grace: Some(grace),
        });
        self
    }

    /// Add a HAVING clause.
    pub fn having(mut self, predicate: Expr) -> Self {
        self.having = Some(predicate);
        self
    }

    /// Write output as a stream.
    pub fn as_stream(self, name: &str, topic: &str) -> SinkBuilder {
        SinkBuilder {
            input: self.build_aggregate(),
            name: name.to_string(),
            topic: topic.to_string(),
            sink_type: SinkType::Stream,
        }
    }

    /// Write output as a table.
    pub fn as_table(self, name: &str, topic: &str) -> SinkBuilder {
        SinkBuilder {
            input: self.build_aggregate(),
            name: name.to_string(),
            topic: topic.to_string(),
            sink_type: SinkType::Table,
        }
    }

    /// Build the aggregate query plan.
    pub fn build(self) -> QueryPlan {
        self.build_aggregate()
    }

    fn build_aggregate(self) -> QueryPlan {
        QueryPlan::Aggregate {
            input: Box::new(self.input),
            group_by: self.group_by,
            aggregates: self.aggregates,
            window: self.window,
            having: self.having,
        }
    }
}

/// Builder for join queries.
pub struct JoinBuilder {
    left: QueryPlan,
    right: QueryPlan,
    join_type: JoinType,
    on: Expr,
    within: Option<Duration>,
}

impl JoinBuilder {
    /// Set the WITHIN window for stream-stream joins.
    pub fn within(mut self, duration: Duration) -> Self {
        self.within = Some(duration);
        self
    }

    /// Continue building from the join result.
    pub fn select(self, expressions: &[Expr]) -> QueryBuilder {
        QueryBuilder {
            plan: QueryPlan::Project {
                input: Box::new(self.build()),
                expressions: expressions.to_vec(),
            },
        }
    }

    /// Add a filter after the join.
    pub fn filter(self, predicate: Expr) -> QueryBuilder {
        QueryBuilder {
            plan: QueryPlan::Filter {
                input: Box::new(self.build()),
                predicate,
            },
        }
    }

    /// Write output as a stream.
    pub fn as_stream(self, name: &str, topic: &str) -> SinkBuilder {
        SinkBuilder {
            input: self.build(),
            name: name.to_string(),
            topic: topic.to_string(),
            sink_type: SinkType::Stream,
        }
    }

    /// Build the join query plan.
    pub fn build(self) -> QueryPlan {
        QueryPlan::Join {
            left: Box::new(self.left),
            right: Box::new(self.right),
            join_type: self.join_type,
            on: self.on,
            within: self.within,
        }
    }
}

/// Builder for sink (output) configuration.
pub struct SinkBuilder {
    input: QueryPlan,
    name: String,
    topic: String,
    sink_type: SinkType,
}

impl SinkBuilder {
    /// Build the complete query plan with sink.
    pub fn build(self) -> QueryPlan {
        QueryPlan::Sink {
            input: Box::new(self.input),
            name: self.name,
            topic: self.topic,
            sink_type: self.sink_type,
        }
    }
}

// Re-export expr helpers for convenient use with builder
pub use crate::expr::{ExprExt, col, lit_bool, lit_f64, lit_i64, lit_null, lit_str};
