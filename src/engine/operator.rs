use anyhow::Result;
use chrono::{DateTime, Utc};

use std::time::Duration;

use crate::engine::state::{AggregateState, JoinBuffer, TableState, WindowKey, WindowState};
use crate::expr::{self, Expr};
use crate::plan::{AggregateExpr, AggregateFunction, JoinType, WindowSpec};
use crate::types::{Column, DataType, Row, RowMetadata, Schema, Value};

/// Trait for stream processing operators.
/// Each operator transforms a batch of rows.
pub trait Operator: Send {
    /// Process a batch of input rows and return output rows.
    fn process(&mut self, rows: Vec<Row>) -> Result<Vec<Row>>;

    /// Flush any buffered state (e.g., final window results).
    fn flush(&mut self) -> Result<Vec<Row>> {
        Ok(vec![])
    }

    /// The output schema of this operator.
    fn output_schema(&self) -> &Schema;

    /// Load right-side rows into a join operator. No-op for non-join operators.
    fn load_right(&mut self, _rows: Vec<Row>, _key_index: usize) {}
}

/// Deserializes JSON record values into typed rows based on a schema.
pub struct DeserializeOp {
    schema: Schema,
}

impl DeserializeOp {
    pub fn new(schema: Schema) -> Self {
        Self { schema }
    }
}

impl Operator for DeserializeOp {
    fn process(&mut self, rows: Vec<Row>) -> Result<Vec<Row>> {
        // Rows come in with a single String value containing JSON.
        // We parse the JSON and extract fields per schema.
        let mut output = Vec::with_capacity(rows.len());
        for row in rows {
            let json_str = match row.get(0) {
                Value::String(s) => s.clone(),
                _ => continue, // Skip non-string values
            };
            let json: serde_json::Value = match serde_json::from_str(&json_str) {
                Ok(v) => v,
                Err(_) => continue, // Skip malformed JSON
            };
            let obj = match &json {
                serde_json::Value::Object(m) => m,
                _ => continue, // Skip non-object JSON
            };
            let mut values = Vec::with_capacity(self.schema.len());
            for col in &self.schema.columns {
                let v = obj
                    .get(&col.name)
                    .or_else(|| {
                        // Try case-insensitive match
                        obj.iter()
                            .find(|(k, _)| k.eq_ignore_ascii_case(&col.name))
                            .map(|(_, v)| v)
                    })
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                values.push(Value::from(v));
            }
            output.push(Row::with_metadata(values, row.metadata));
        }
        Ok(output)
    }

    fn output_schema(&self) -> &Schema {
        &self.schema
    }
}

/// Filters rows based on a predicate expression.
pub struct FilterOp {
    predicate: Expr,
    schema: Schema,
}

impl FilterOp {
    pub fn new(predicate: Expr, schema: Schema) -> Self {
        Self { predicate, schema }
    }
}

impl Operator for FilterOp {
    fn process(&mut self, rows: Vec<Row>) -> Result<Vec<Row>> {
        let mut output = Vec::new();
        for row in rows {
            let result = expr::eval(&self.predicate, &row, &self.schema)?;
            if result.is_truthy() {
                output.push(row);
            }
        }
        Ok(output)
    }

    fn output_schema(&self) -> &Schema {
        &self.schema
    }
}

/// Projects rows to a new schema based on expressions.
pub struct ProjectOp {
    expressions: Vec<Expr>,
    input_schema: Schema,
    output_schema: Schema,
}

impl ProjectOp {
    pub fn new(expressions: Vec<Expr>, input_schema: Schema) -> Self {
        let output_schema = compute_projection_schema(&expressions, &input_schema);
        Self {
            expressions,
            input_schema,
            output_schema,
        }
    }
}

impl Operator for ProjectOp {
    fn process(&mut self, rows: Vec<Row>) -> Result<Vec<Row>> {
        let mut output = Vec::with_capacity(rows.len());
        for row in rows {
            // Handle wildcard
            if self.expressions.len() == 1 && self.expressions[0] == Expr::Wildcard {
                output.push(row);
                continue;
            }

            let mut values = Vec::with_capacity(self.expressions.len());
            for expr in &self.expressions {
                if matches!(expr, Expr::Wildcard) {
                    values.extend(row.values.clone());
                } else {
                    values.push(expr::eval(expr, &row, &self.input_schema)?);
                }
            }
            output.push(Row::with_metadata(values, row.metadata));
        }
        Ok(output)
    }

    fn output_schema(&self) -> &Schema {
        &self.output_schema
    }
}

/// Computes the output schema for a projection.
fn compute_projection_schema(expressions: &[Expr], input_schema: &Schema) -> Schema {
    let mut columns = Vec::new();
    for expr in expressions {
        match expr {
            Expr::Wildcard => {
                columns.extend(input_schema.columns.clone());
            }
            Expr::Alias { name, .. } => {
                columns.push(Column::new(name.clone(), DataType::String));
            }
            Expr::Column(name) => {
                if let Some(col) = input_schema.column(name) {
                    columns.push(col.clone());
                } else {
                    columns.push(Column::new(name.clone(), DataType::String));
                }
            }
            _ => {
                columns.push(Column::new("_expr", DataType::String));
            }
        }
    }
    Schema::new(columns)
}

/// Non-windowed GROUP BY aggregation operator.
/// Emits changelog-style results: one row per group with latest aggregate values.
pub struct AggregateOp {
    group_by: Vec<Expr>,
    aggregates: Vec<AggregateExpr>,
    having: Option<Expr>,
    input_schema: Schema,
    output_schema: Schema,
    state: AggregateState,
}

impl AggregateOp {
    pub fn new(
        group_by: Vec<Expr>,
        aggregates: Vec<AggregateExpr>,
        having: Option<Expr>,
        input_schema: Schema,
    ) -> Self {
        let output_schema = compute_aggregate_schema(&group_by, &aggregates, &input_schema);
        Self {
            group_by,
            aggregates,
            having,
            input_schema,
            output_schema,
            state: AggregateState::new(),
        }
    }
}

impl Operator for AggregateOp {
    fn process(&mut self, rows: Vec<Row>) -> Result<Vec<Row>> {
        let functions: Vec<(AggregateFunction, bool)> = self
            .aggregates
            .iter()
            .map(|a| (a.function.clone(), a.distinct))
            .collect();

        // Accumulate all rows into groups
        for row in &rows {
            let group_key: Vec<Value> = self
                .group_by
                .iter()
                .map(|e| expr::eval(e, row, &self.input_schema))
                .collect::<Result<_>>()?;

            let accumulators = self.state.get_or_create(group_key, &functions);

            for (i, agg_expr) in self.aggregates.iter().enumerate() {
                if matches!(agg_expr.expr, Expr::Wildcard) {
                    accumulators[i].accumulate_star();
                } else {
                    let val = expr::eval(&agg_expr.expr, row, &self.input_schema)?;
                    accumulators[i].accumulate(&val);
                }
            }
        }

        // Emit current state of all groups
        let mut output = Vec::new();
        for (group_key, accumulators) in self.state.iter() {
            let mut values: Vec<Value> = group_key.clone();
            for acc in accumulators {
                values.push(acc.result());
            }
            let row = Row::new(values);

            // Apply HAVING filter if present
            if let Some(having) = &self.having {
                let result = expr::eval(having, &row, &self.output_schema)?;
                if !result.is_truthy() {
                    continue;
                }
            }

            output.push(row);
        }

        Ok(output)
    }

    fn output_schema(&self) -> &Schema {
        &self.output_schema
    }
}

/// Computes the output schema for an aggregate query.
pub fn compute_aggregate_schema(
    group_by: &[Expr],
    aggregates: &[AggregateExpr],
    input_schema: &Schema,
) -> Schema {
    let mut columns = Vec::new();

    // Group-by columns come first
    for expr in group_by {
        match expr {
            Expr::Column(name) => {
                if let Some(col) = input_schema.column(name) {
                    columns.push(col.clone());
                } else {
                    columns.push(Column::new(name.clone(), DataType::String));
                }
            }
            Expr::Alias { name, .. } => {
                columns.push(Column::new(name.clone(), DataType::String));
            }
            _ => {
                columns.push(Column::new("_group", DataType::String));
            }
        }
    }

    // Aggregate columns
    for agg in aggregates {
        let data_type = match &agg.function {
            AggregateFunction::Count => DataType::Integer,
            AggregateFunction::Sum | AggregateFunction::Avg => DataType::Double,
            AggregateFunction::Min | AggregateFunction::Max => {
                // Try to infer from input
                if let Expr::Column(name) = &agg.expr {
                    input_schema
                        .data_type(name)
                        .cloned()
                        .unwrap_or(DataType::Double)
                } else {
                    DataType::Double
                }
            }
            AggregateFunction::CollectList
            | AggregateFunction::CollectSet
            | AggregateFunction::TopK(_) => DataType::Array(Box::new(DataType::String)),
        };
        columns.push(Column::new(agg.alias.clone(), data_type));
    }

    Schema::new(columns)
}

/// Maintains latest-per-key state and emits changelog.
pub struct TableSinkOp {
    key_index: usize,
    schema: Schema,
    state: TableState,
}

impl TableSinkOp {
    pub fn new(key_index: usize, schema: Schema) -> Self {
        Self {
            key_index,
            schema,
            state: TableState::new(),
        }
    }

    pub fn state(&self) -> &TableState {
        &self.state
    }
}

impl Operator for TableSinkOp {
    fn process(&mut self, rows: Vec<Row>) -> Result<Vec<Row>> {
        let mut output = Vec::new();
        for row in rows {
            let key = row.get(self.key_index).clone();
            self.state.upsert(key, row.clone());
            output.push(row);
        }
        Ok(output)
    }

    fn output_schema(&self) -> &Schema {
        &self.schema
    }
}

/// Windowed GROUP BY aggregation operator.
/// Routes each row to its time window(s), then aggregates per (group_key, window).
pub struct WindowedAggregateOp {
    group_by: Vec<Expr>,
    aggregates: Vec<AggregateExpr>,
    having: Option<Expr>,
    window_spec: WindowSpec,
    input_schema: Schema,
    output_schema: Schema,
    state: WindowState,
}

impl WindowedAggregateOp {
    pub fn new(
        group_by: Vec<Expr>,
        aggregates: Vec<AggregateExpr>,
        having: Option<Expr>,
        window_spec: WindowSpec,
        input_schema: Schema,
    ) -> Self {
        let output_schema = compute_aggregate_schema(&group_by, &aggregates, &input_schema);
        Self {
            group_by,
            aggregates,
            having,
            window_spec,
            input_schema,
            output_schema,
            state: WindowState::new(),
        }
    }

    /// Compute the window boundaries a timestamp falls into.
    fn windows_for_timestamp(&self, ts: DateTime<Utc>) -> Vec<(DateTime<Utc>, DateTime<Utc>)> {
        match &self.window_spec {
            WindowSpec::Tumbling { size, .. } => {
                let size_millis = size.as_millis() as i64;
                let ts_millis = ts.timestamp_millis();
                let window_start_millis = (ts_millis / size_millis) * size_millis;
                let start = DateTime::from_timestamp_millis(window_start_millis).unwrap();
                let end =
                    DateTime::from_timestamp_millis(window_start_millis + size_millis).unwrap();
                vec![(start, end)]
            }
            WindowSpec::Hopping { size, advance, .. } => {
                let size_millis = size.as_millis() as i64;
                let advance_millis = advance.as_millis() as i64;
                let ts_millis = ts.timestamp_millis();

                let mut windows = Vec::new();
                // Find all windows this timestamp falls into
                // A window starting at S covers [S, S+size)
                // Windows start at ..., -2*advance, -advance, 0, advance, 2*advance, ...
                let latest_start = (ts_millis / advance_millis) * advance_millis;
                let mut start_millis = latest_start;

                while start_millis + size_millis > ts_millis {
                    if start_millis <= ts_millis {
                        let start = DateTime::from_timestamp_millis(start_millis).unwrap();
                        let end =
                            DateTime::from_timestamp_millis(start_millis + size_millis).unwrap();
                        windows.push((start, end));
                    }
                    start_millis -= advance_millis;
                }
                windows
            }
            WindowSpec::Session { .. } => {
                // Session windows are created/merged dynamically
                // For simplicity, each event starts its own window
                // The merge happens at flush time
                let ts_millis = ts.timestamp_millis();
                let start = DateTime::from_timestamp_millis(ts_millis).unwrap();
                let end = start; // Will be extended by gap
                vec![(start, end)]
            }
        }
    }
}

impl Operator for WindowedAggregateOp {
    fn process(&mut self, rows: Vec<Row>) -> Result<Vec<Row>> {
        let functions: Vec<(AggregateFunction, bool)> = self
            .aggregates
            .iter()
            .map(|a| (a.function.clone(), a.distinct))
            .collect();

        for row in &rows {
            let ts = row.metadata.timestamp.unwrap_or_else(Utc::now);

            let group_key: Vec<Value> = self
                .group_by
                .iter()
                .map(|e| expr::eval(e, row, &self.input_schema))
                .collect::<Result<_>>()?;

            let windows = self.windows_for_timestamp(ts);

            for (window_start, window_end) in windows {
                let wk = WindowKey {
                    group: group_key.clone(),
                    window_start,
                };

                let (accumulators, _, _) = self.state.get_or_create(wk, window_end, &functions);

                for (i, agg_expr) in self.aggregates.iter().enumerate() {
                    if matches!(agg_expr.expr, Expr::Wildcard) {
                        accumulators[i].accumulate_star();
                    } else {
                        let val = expr::eval(&agg_expr.expr, row, &self.input_schema)?;
                        accumulators[i].accumulate(&val);
                    }
                }
            }
        }

        // Emit current state of all windows
        let mut output = Vec::new();
        for (wk, (accumulators, start, end)) in self.state.iter() {
            let mut values: Vec<Value> = wk.group.clone();
            for acc in accumulators {
                values.push(acc.result());
            }

            let metadata = RowMetadata {
                window_start: Some(*start),
                window_end: Some(*end),
                ..Default::default()
            };
            let row = Row::with_metadata(values, metadata);

            // Apply HAVING filter if present
            if let Some(having) = &self.having {
                let result = expr::eval(having, &row, &self.output_schema)?;
                if !result.is_truthy() {
                    continue;
                }
            }

            output.push(row);
        }

        Ok(output)
    }

    fn output_schema(&self) -> &Schema {
        &self.output_schema
    }
}

// === Join Helpers ===

/// Combine two rows into one (left columns followed by right columns).
fn combine_rows(left: &Row, right: &Row) -> Row {
    let mut values = left.values.clone();
    values.extend(right.values.clone());
    Row::with_metadata(values, left.metadata.clone())
}

/// Create a row of NULLs matching the given schema width.
fn null_row(schema: &Schema) -> Row {
    let values = vec![Value::Null; schema.len()];
    Row::new(values)
}

/// Combine two schemas into one (left columns followed by right columns).
fn combine_schemas(left: &Schema, right: &Schema) -> Schema {
    let mut columns = left.columns.clone();
    columns.extend(right.columns.clone());
    Schema::new(columns)
}

/// Extracts join key column indices from a join ON expression (ON left.col = right.col).
pub fn extract_join_key_indices(
    on: &Expr,
    left_schema: &Schema,
    right_schema: &Schema,
) -> Option<(usize, usize)> {
    if let Expr::BinaryOp {
        left,
        op: crate::expr::BinaryOp::Eq,
        right,
    } = on
    {
        if let (Expr::Column(left_col), Expr::Column(right_col)) = (left.as_ref(), right.as_ref()) {
            let li = left_schema.index_of(left_col);
            let ri = right_schema.index_of(right_col);
            if let (Some(li), Some(ri)) = (li, ri) {
                return Some((li, ri));
            }
        }
    }
    None
}

/// Stream-table join: for each left (stream) row, look up matching right (table) rows.
pub struct StreamTableJoinOp {
    join_type: JoinType,
    right_schema: Schema,
    output_schema_val: Schema,
    right_state: TableState,
    left_key_index: usize,
}

impl StreamTableJoinOp {
    pub fn new(join_type: JoinType, on: Expr, left_schema: Schema, right_schema: Schema) -> Self {
        let output_schema_val = combine_schemas(&left_schema, &right_schema);
        let (left_key_index, _right_key_index) =
            extract_join_key_indices(&on, &left_schema, &right_schema).unwrap_or((0, 0));
        Self {
            join_type,
            right_schema,
            output_schema_val,
            right_state: TableState::new(),
            left_key_index,
        }
    }

    /// Populate the right-side table state from rows.
    pub fn load_right(&mut self, rows: Vec<Row>, key_index: usize) {
        for row in rows {
            let key = row.get(key_index).clone();
            self.right_state.upsert(key, row);
        }
    }
}

impl Operator for StreamTableJoinOp {
    fn process(&mut self, rows: Vec<Row>) -> Result<Vec<Row>> {
        let mut output = Vec::new();
        for left_row in &rows {
            let left_key = left_row.get(self.left_key_index);
            if let Some(right_row) = self.right_state.get(left_key) {
                output.push(combine_rows(left_row, right_row));
            } else {
                match self.join_type {
                    JoinType::Left | JoinType::FullOuter => {
                        let null_right = null_row(&self.right_schema);
                        output.push(combine_rows(left_row, &null_right));
                    }
                    _ => {}
                }
            }
        }
        Ok(output)
    }

    fn output_schema(&self) -> &Schema {
        &self.output_schema_val
    }

    fn load_right(&mut self, rows: Vec<Row>, key_index: usize) {
        StreamTableJoinOp::load_right(self, rows, key_index);
    }
}

/// Stream-stream join: buffers both sides within a time window.
pub struct StreamStreamJoinOp {
    join_type: JoinType,
    within: Duration,
    left_schema: Schema,
    right_schema: Schema,
    output_schema_val: Schema,
    left_buffer: JoinBuffer,
    right_buffer: JoinBuffer,
    left_key_index: usize,
    right_key_index: usize,
}

impl StreamStreamJoinOp {
    pub fn new(
        join_type: JoinType,
        on: Expr,
        within: Duration,
        left_schema: Schema,
        right_schema: Schema,
    ) -> Self {
        let output_schema_val = combine_schemas(&left_schema, &right_schema);
        let (left_key_index, right_key_index) =
            extract_join_key_indices(&on, &left_schema, &right_schema).unwrap_or((0, 0));
        Self {
            join_type,
            within,
            left_schema,
            right_schema,
            output_schema_val,
            left_buffer: JoinBuffer::new(),
            right_buffer: JoinBuffer::new(),
            left_key_index,
            right_key_index,
        }
    }

    /// Process a batch of left-side rows.
    pub fn process_left(&mut self, rows: Vec<Row>) -> Result<Vec<Row>> {
        let mut output = Vec::new();
        for left_row in rows {
            let ts = left_row.metadata.timestamp.unwrap_or_else(Utc::now);
            let left_key = left_row.get(self.left_key_index).clone();

            let from =
                ts - chrono::Duration::from_std(self.within).unwrap_or(chrono::Duration::zero());
            let to =
                ts + chrono::Duration::from_std(self.within).unwrap_or(chrono::Duration::zero());

            let mut matched = false;
            for right_row in self.right_buffer.range(from, to) {
                let right_key = right_row.get(self.right_key_index);
                if &left_key == right_key {
                    output.push(combine_rows(&left_row, right_row));
                    matched = true;
                }
            }

            if !matched {
                match self.join_type {
                    JoinType::Left | JoinType::FullOuter => {
                        let null_right = null_row(&self.right_schema);
                        output.push(combine_rows(&left_row, &null_right));
                    }
                    _ => {}
                }
            }

            self.left_buffer.insert(ts, left_row);
        }
        Ok(output)
    }

    /// Process a batch of right-side rows.
    pub fn process_right(&mut self, rows: Vec<Row>) -> Result<Vec<Row>> {
        let mut output = Vec::new();
        for right_row in rows {
            let ts = right_row.metadata.timestamp.unwrap_or_else(Utc::now);
            let right_key = right_row.get(self.right_key_index).clone();

            let from =
                ts - chrono::Duration::from_std(self.within).unwrap_or(chrono::Duration::zero());
            let to =
                ts + chrono::Duration::from_std(self.within).unwrap_or(chrono::Duration::zero());

            let mut matched = false;
            for left_row in self.left_buffer.range(from, to) {
                let left_key = left_row.get(self.left_key_index);
                if &right_key == left_key {
                    output.push(combine_rows(left_row, &right_row));
                    matched = true;
                }
            }

            if !matched {
                match self.join_type {
                    JoinType::Right | JoinType::FullOuter => {
                        let null_left = null_row(&self.left_schema);
                        output.push(combine_rows(&null_left, &right_row));
                    }
                    _ => {}
                }
            }

            self.right_buffer.insert(ts, right_row);
        }
        Ok(output)
    }
}

impl Operator for StreamStreamJoinOp {
    fn process(&mut self, rows: Vec<Row>) -> Result<Vec<Row>> {
        self.process_left(rows)
    }

    fn output_schema(&self) -> &Schema {
        &self.output_schema_val
    }
}

/// Table-table join: both sides are fully materialized.
pub struct TableTableJoinOp {
    join_type: JoinType,
    left_schema: Schema,
    right_schema: Schema,
    output_schema_val: Schema,
    left_state: TableState,
    right_state: TableState,
    left_key_index: usize,
}

impl TableTableJoinOp {
    pub fn new(join_type: JoinType, on: Expr, left_schema: Schema, right_schema: Schema) -> Self {
        let output_schema_val = combine_schemas(&left_schema, &right_schema);
        let (left_key_index, _right_key_index) =
            extract_join_key_indices(&on, &left_schema, &right_schema).unwrap_or((0, 0));
        Self {
            join_type,
            left_schema,
            right_schema,
            output_schema_val,
            left_state: TableState::new(),
            right_state: TableState::new(),
            left_key_index,
        }
    }

    /// Load left-side table state.
    pub fn load_left(&mut self, rows: Vec<Row>) {
        for row in rows {
            let key = row.get(self.left_key_index).clone();
            self.left_state.upsert(key, row);
        }
    }

    /// Load right-side table state.
    pub fn load_right(&mut self, rows: Vec<Row>, key_index: usize) {
        for row in rows {
            let key = row.get(key_index).clone();
            self.right_state.upsert(key, row);
        }
    }

    /// Perform the full join between both materialized tables.
    pub fn join_all(&self) -> Vec<Row> {
        let mut output = Vec::new();
        let mut right_matched: std::collections::HashSet<Value> = std::collections::HashSet::new();

        for (left_key, left_row) in self.left_state.iter() {
            if let Some(right_row) = self.right_state.get(left_key) {
                output.push(combine_rows(left_row, right_row));
                right_matched.insert(left_key.clone());
            } else {
                match self.join_type {
                    JoinType::Left | JoinType::FullOuter => {
                        let null_right = null_row(&self.right_schema);
                        output.push(combine_rows(left_row, &null_right));
                    }
                    _ => {}
                }
            }
        }

        if matches!(self.join_type, JoinType::Right | JoinType::FullOuter) {
            for (right_key, right_row) in self.right_state.iter() {
                if !right_matched.contains(right_key) {
                    let null_left = null_row(&self.left_schema);
                    output.push(combine_rows(&null_left, right_row));
                }
            }
        }

        output
    }
}

impl Operator for TableTableJoinOp {
    fn process(&mut self, rows: Vec<Row>) -> Result<Vec<Row>> {
        self.load_left(rows);
        Ok(self.join_all())
    }

    fn output_schema(&self) -> &Schema {
        &self.output_schema_val
    }

    fn load_right(&mut self, rows: Vec<Row>, key_index: usize) {
        TableTableJoinOp::load_right(self, rows, key_index);
    }
}
