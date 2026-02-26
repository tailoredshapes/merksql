use anyhow::Result;

use crate::engine::operator::{
    AggregateOp, DeserializeOp, FilterOp, Operator, ProjectOp, StreamStreamJoinOp,
    StreamTableJoinOp, WindowedAggregateOp, extract_join_key_indices,
};
use crate::plan::QueryPlan;
use crate::schema::SchemaRegistry;
use crate::types::{Row, Schema};

/// A compiled pipeline: a chain of operators with source topic(s) and output schema.
pub struct Pipeline {
    pub source_topics: Vec<String>,
    pub operators: Vec<Box<dyn Operator>>,
    pub output_schema: Schema,
    /// For join queries: the right-side source topic.
    pub right_source_topic: Option<String>,
    /// For join queries: the right-side schema (for deserialization).
    pub right_schema: Option<Schema>,
    /// For join queries: index of the join operator in `operators`.
    pub join_op_index: Option<usize>,
    /// For join queries: the key column index in the right-side schema.
    pub right_key_index: Option<usize>,
}

impl std::fmt::Debug for Pipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pipeline")
            .field("source_topics", &self.source_topics)
            .field("operators_count", &self.operators.len())
            .field("output_schema", &self.output_schema)
            .field("right_source_topic", &self.right_source_topic)
            .finish()
    }
}

impl Pipeline {
    /// Process a batch of rows through all operators in sequence.
    pub fn process(&mut self, mut rows: Vec<Row>) -> Result<Vec<Row>> {
        for op in &mut self.operators {
            rows = op.process(rows)?;
            if rows.is_empty() {
                return Ok(rows);
            }
        }
        Ok(rows)
    }

    /// Load right-side rows into the join operator, if this is a join pipeline.
    pub fn load_join_right(&mut self, rows: Vec<Row>) {
        if let (Some(idx), Some(key_idx)) = (self.join_op_index, self.right_key_index) {
            self.operators[idx].load_right(rows, key_idx);
        }
    }

    /// Flush all operators and return any remaining results.
    pub fn flush(&mut self) -> Result<Vec<Row>> {
        let mut result = Vec::new();
        for op in &mut self.operators {
            let flushed = op.flush()?;
            if !flushed.is_empty() {
                result.extend(flushed);
            }
        }
        Ok(result)
    }
}

/// Compile a QueryPlan into a Pipeline using the schema registry.
pub fn compile(plan: &QueryPlan, registry: &SchemaRegistry) -> Result<Pipeline> {
    let source_topics = resolve_source_topics(plan, registry)?;
    let (operators, output_schema) = compile_plan(plan, registry)?;

    // Extract join metadata from the plan
    let join_meta = extract_join_metadata(plan, registry, operators.len());

    Ok(Pipeline {
        source_topics,
        operators,
        output_schema,
        right_source_topic: join_meta.0,
        right_schema: join_meta.1,
        join_op_index: join_meta.2,
        right_key_index: join_meta.3,
    })
}

/// Resolve source names to topic names using the registry.
fn resolve_source_topics(plan: &QueryPlan, registry: &SchemaRegistry) -> Result<Vec<String>> {
    let source_names = plan.source_names();
    let mut topics = Vec::new();
    for name in &source_names {
        let info = registry
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("Unknown source: {}", name))?;
        topics.push(info.topic.clone());
    }
    Ok(topics)
}

/// Recursively compile a query plan into operators.
fn compile_plan(
    plan: &QueryPlan,
    registry: &SchemaRegistry,
) -> Result<(Vec<Box<dyn Operator>>, Schema)> {
    match plan {
        QueryPlan::Scan { source } => {
            let info = registry
                .get(source)
                .ok_or_else(|| anyhow::anyhow!("Unknown source: {}", source))?;
            let schema = info.schema.clone();
            let deser = DeserializeOp::new(schema.clone());
            Ok((vec![Box::new(deser)], schema))
        }

        QueryPlan::Filter { input, predicate } => {
            let (mut operators, schema) = compile_plan(input, registry)?;
            let filter = FilterOp::new(predicate.clone(), schema.clone());
            operators.push(Box::new(filter));
            Ok((operators, schema))
        }

        QueryPlan::Project { input, expressions } => {
            let (mut operators, schema) = compile_plan(input, registry)?;
            let project = ProjectOp::new(expressions.clone(), schema);
            let output_schema = project.output_schema().clone();
            operators.push(Box::new(project));
            Ok((operators, output_schema))
        }

        QueryPlan::Aggregate {
            input,
            group_by,
            aggregates,
            having,
            window: None,
        } => {
            let (mut operators, schema) = compile_plan(input, registry)?;
            let agg_op =
                AggregateOp::new(group_by.clone(), aggregates.clone(), having.clone(), schema);
            let output_schema = agg_op.output_schema().clone();
            operators.push(Box::new(agg_op));
            Ok((operators, output_schema))
        }

        QueryPlan::Aggregate {
            input,
            group_by,
            aggregates,
            having,
            window: Some(window_spec),
        } => {
            let (mut operators, schema) = compile_plan(input, registry)?;
            let windowed_op = WindowedAggregateOp::new(
                group_by.clone(),
                aggregates.clone(),
                having.clone(),
                window_spec.clone(),
                schema,
            );
            let output_schema = windowed_op.output_schema().clone();
            operators.push(Box::new(windowed_op));
            Ok((operators, output_schema))
        }

        QueryPlan::Join {
            left,
            right,
            join_type,
            on,
            within,
        } => {
            let (left_operators, left_schema) = compile_plan(left, registry)?;
            let (_right_operators, right_schema) = compile_plan(right, registry)?;

            // For the pipeline model, we compile both sides and use a join operator.
            // The left side operators deserialize left rows, right side operators deserialize right rows.
            // We merge left operators into the pipeline, then add the join operator.
            let mut operators = left_operators;

            if within.is_some() {
                // Stream-stream join with WITHIN window
                let join_op = StreamStreamJoinOp::new(
                    *join_type,
                    on.clone(),
                    within.unwrap(),
                    left_schema.clone(),
                    right_schema.clone(),
                );
                let output_schema = join_op.output_schema().clone();
                operators.push(Box::new(join_op));
                Ok((operators, output_schema))
            } else {
                // Stream-table join (default when no WITHIN)
                let join_op = StreamTableJoinOp::new(
                    *join_type,
                    on.clone(),
                    left_schema.clone(),
                    right_schema.clone(),
                );
                let output_schema = join_op.output_schema().clone();
                operators.push(Box::new(join_op));
                Ok((operators, output_schema))
            }
        }

        QueryPlan::Sink { input, .. } => {
            // Sink is handled at a higher level; just compile the input
            compile_plan(input, registry)
        }
    }
}

/// Walk the plan to find a Join node and extract metadata needed for pull/persistent queries.
/// Returns (right_topic, right_schema, join_op_index, right_key_index).
fn extract_join_metadata(
    plan: &QueryPlan,
    registry: &SchemaRegistry,
    _total_ops: usize,
) -> (Option<String>, Option<Schema>, Option<usize>, Option<usize>) {
    extract_join_metadata_inner(plan, registry)
}

fn extract_join_metadata_inner(
    plan: &QueryPlan,
    registry: &SchemaRegistry,
) -> (Option<String>, Option<Schema>, Option<usize>, Option<usize>) {
    match plan {
        QueryPlan::Join {
            left, right, on, ..
        } => {
            // Resolve right source topic
            let right_names = right.source_names();
            if let Some(right_name) = right_names.first() {
                if let Some(right_info) = registry.get(right_name) {
                    let right_topic = right_info.topic.clone();
                    let right_schema = right_info.schema.clone();

                    // Figure out right key index from the ON expression
                    let left_names = left.source_names();
                    let left_schema = left_names
                        .first()
                        .and_then(|n| registry.get(n))
                        .map(|i| i.schema.clone())
                        .unwrap_or_else(Schema::empty);

                    let (_left_key, right_key) =
                        extract_join_key_indices(on, &left_schema, &right_schema).unwrap_or((0, 0));

                    // Count left-side operators to know where join op sits
                    let left_op_count = count_operators(left, registry);
                    let join_op_idx = left_op_count; // join op is appended right after left ops

                    return (
                        Some(right_topic),
                        Some(right_schema),
                        Some(join_op_idx),
                        Some(right_key),
                    );
                }
            }
            (None, None, None, None)
        }
        QueryPlan::Filter { input, .. }
        | QueryPlan::Project { input, .. }
        | QueryPlan::Aggregate { input, .. }
        | QueryPlan::Sink { input, .. } => extract_join_metadata_inner(input, registry),
        QueryPlan::Scan { .. } => (None, None, None, None),
    }
}

/// Count operators that compile_plan would produce for a given plan.
fn count_operators(plan: &QueryPlan, registry: &SchemaRegistry) -> usize {
    match plan {
        QueryPlan::Scan { .. } => 1, // DeserializeOp
        QueryPlan::Filter { input, .. }
        | QueryPlan::Project { input, .. }
        | QueryPlan::Aggregate { input, .. } => count_operators(input, registry) + 1,
        QueryPlan::Join { left, .. } => {
            count_operators(left, registry) + 1 // left ops + join op
        }
        QueryPlan::Sink { input, .. } => count_operators(input, registry),
    }
}
