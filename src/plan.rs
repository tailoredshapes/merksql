use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::expr::Expr;

mod duration_millis {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        d.as_millis().serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        let millis = u64::deserialize(d)?;
        Ok(Duration::from_millis(millis))
    }
}

mod option_duration_millis {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(opt: &Option<Duration>, s: S) -> Result<S::Ok, S::Error> {
        match opt {
            Some(d) => d.as_millis().serialize(s),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
        let opt: Option<u64> = Option::deserialize(d)?;
        Ok(opt.map(Duration::from_millis))
    }
}

/// Aggregate function types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    CollectList,
    CollectSet,
    TopK(usize),
}

/// An aggregate expression: function applied to an expression with optional alias.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregateExpr {
    pub function: AggregateFunction,
    pub expr: Expr,
    pub alias: String,
    pub distinct: bool,
}

/// Window specification for time-based aggregation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WindowSpec {
    Tumbling {
        #[serde(with = "duration_millis")]
        size: Duration,
        #[serde(with = "option_duration_millis")]
        grace: Option<Duration>,
    },
    Hopping {
        #[serde(with = "duration_millis")]
        size: Duration,
        #[serde(with = "duration_millis")]
        advance: Duration,
        #[serde(with = "option_duration_millis")]
        grace: Option<Duration>,
    },
    Session {
        #[serde(with = "duration_millis")]
        gap: Duration,
        #[serde(with = "option_duration_millis")]
        grace: Option<Duration>,
    },
}

/// Join types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    FullOuter,
}

/// Sink types — whether output is a stream or table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SinkType {
    Stream,
    Table,
}

/// Query plan intermediate representation.
/// Plans are composed by nesting: outer nodes wrap inner nodes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QueryPlan {
    /// Scan a source stream/table by name.
    Scan { source: String },
    /// Filter rows matching a predicate.
    Filter {
        input: Box<QueryPlan>,
        predicate: Expr,
    },
    /// Project specific expressions (SELECT columns/expressions).
    Project {
        input: Box<QueryPlan>,
        expressions: Vec<Expr>,
    },
    /// Aggregate with GROUP BY and aggregate functions.
    Aggregate {
        input: Box<QueryPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<AggregateExpr>,
        window: Option<WindowSpec>,
        having: Option<Expr>,
    },
    /// Join two sources.
    Join {
        left: Box<QueryPlan>,
        right: Box<QueryPlan>,
        join_type: JoinType,
        on: Expr,
        #[serde(with = "option_duration_millis")]
        within: Option<Duration>,
    },
    /// Write results to an output topic as stream or table.
    Sink {
        input: Box<QueryPlan>,
        name: String,
        topic: String,
        sink_type: SinkType,
    },
}

impl QueryPlan {
    /// Get the source names that this plan reads from (leaf Scan nodes).
    pub fn source_names(&self) -> Vec<String> {
        match self {
            QueryPlan::Scan { source } => vec![source.clone()],
            QueryPlan::Filter { input, .. }
            | QueryPlan::Project { input, .. }
            | QueryPlan::Aggregate { input, .. }
            | QueryPlan::Sink { input, .. } => input.source_names(),
            QueryPlan::Join { left, right, .. } => {
                let mut sources = left.source_names();
                sources.extend(right.source_names());
                sources
            }
        }
    }
}
