use std::collections::{BTreeMap, BTreeSet, HashMap};

use chrono::{DateTime, Utc};

use crate::plan::AggregateFunction;
use crate::types::{Row, Value};

/// Maintains latest-per-key state for tables and stream-table join lookups.
#[derive(Debug, Default)]
pub struct TableState {
    state: HashMap<Value, Row>,
}

impl TableState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert(&mut self, key: Value, row: Row) {
        self.state.insert(key, row);
    }

    pub fn get(&self, key: &Value) -> Option<&Row> {
        self.state.get(key)
    }

    pub fn remove(&mut self, key: &Value) -> Option<Row> {
        self.state.remove(key)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Value, &Row)> {
        self.state.iter()
    }

    pub fn len(&self) -> usize {
        self.state.len()
    }

    pub fn is_empty(&self) -> bool {
        self.state.is_empty()
    }
}

/// Accumulates values for a single aggregate function over a group.
#[derive(Debug, Clone)]
pub struct Accumulator {
    pub function: AggregateFunction,
    pub count: i64,
    pub sum: f64,
    pub min: Option<Value>,
    pub max: Option<Value>,
    pub list: Vec<Value>,
    pub set: BTreeSet<Value>,
    pub distinct: bool,
    seen: BTreeSet<Value>,
}

impl Accumulator {
    pub fn new(function: AggregateFunction, distinct: bool) -> Self {
        Self {
            function,
            count: 0,
            sum: 0.0,
            min: None,
            max: None,
            list: Vec::new(),
            set: BTreeSet::new(),
            distinct,
            seen: BTreeSet::new(),
        }
    }

    pub fn accumulate(&mut self, value: &Value) {
        if value.is_null() {
            // COUNT(*) still counts nulls, but COUNT(expr) doesn't
            if matches!(self.function, AggregateFunction::Count) && !matches!(value, Value::Null) {
                // This branch won't be reached since we checked is_null above
            }
            return;
        }

        if self.distinct {
            if self.seen.contains(value) {
                return;
            }
            self.seen.insert(value.clone());
        }

        match &self.function {
            AggregateFunction::Count => {
                self.count += 1;
            }
            AggregateFunction::Sum | AggregateFunction::Avg => {
                self.count += 1;
                if let Some(f) = value.as_f64() {
                    self.sum += f;
                }
            }
            AggregateFunction::Min => {
                self.min = Some(match &self.min {
                    None => value.clone(),
                    Some(current) => {
                        if value < current {
                            value.clone()
                        } else {
                            current.clone()
                        }
                    }
                });
            }
            AggregateFunction::Max => {
                self.max = Some(match &self.max {
                    None => value.clone(),
                    Some(current) => {
                        if value > current {
                            value.clone()
                        } else {
                            current.clone()
                        }
                    }
                });
            }
            AggregateFunction::CollectList => {
                self.list.push(value.clone());
            }
            AggregateFunction::CollectSet => {
                self.set.insert(value.clone());
            }
            AggregateFunction::TopK(k) => {
                self.list.push(value.clone());
                self.list.sort();
                self.list.reverse();
                self.list.truncate(*k);
            }
        }
    }

    /// Count a row for COUNT(*) — counts regardless of null.
    pub fn accumulate_star(&mut self) {
        self.count += 1;
    }

    pub fn result(&self) -> Value {
        match &self.function {
            AggregateFunction::Count => Value::Integer(self.count),
            AggregateFunction::Sum => {
                if self.count == 0 {
                    Value::Null
                } else {
                    Value::Double(self.sum)
                }
            }
            AggregateFunction::Avg => {
                if self.count == 0 {
                    Value::Null
                } else {
                    Value::Double(self.sum / self.count as f64)
                }
            }
            AggregateFunction::Min => self.min.clone().unwrap_or(Value::Null),
            AggregateFunction::Max => self.max.clone().unwrap_or(Value::Null),
            AggregateFunction::CollectList => Value::Array(self.list.clone()),
            AggregateFunction::CollectSet => Value::Array(self.set.iter().cloned().collect()),
            AggregateFunction::TopK(_) => Value::Array(self.list.clone()),
        }
    }

    pub fn reset(&mut self) {
        self.count = 0;
        self.sum = 0.0;
        self.min = None;
        self.max = None;
        self.list.clear();
        self.set.clear();
        self.seen.clear();
    }
}

/// Per-group accumulator set for non-windowed GROUP BY.
#[derive(Debug, Default)]
pub struct AggregateState {
    groups: HashMap<Vec<Value>, Vec<Accumulator>>,
}

impl AggregateState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_or_create(
        &mut self,
        key: Vec<Value>,
        functions: &[(AggregateFunction, bool)],
    ) -> &mut Vec<Accumulator> {
        self.groups.entry(key).or_insert_with(|| {
            functions
                .iter()
                .map(|(f, distinct)| Accumulator::new(f.clone(), *distinct))
                .collect()
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Vec<Value>, &Vec<Accumulator>)> {
        self.groups.iter()
    }

    pub fn len(&self) -> usize {
        self.groups.len()
    }

    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    pub fn clear(&mut self) {
        self.groups.clear();
    }
}

/// Key for windowed state: (group_key, window_start).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WindowKey {
    pub group: Vec<Value>,
    pub window_start: DateTime<Utc>,
}

/// Per-window accumulator set for windowed GROUP BY.
#[derive(Debug, Default)]
pub struct WindowState {
    windows: HashMap<WindowKey, (Vec<Accumulator>, DateTime<Utc>, DateTime<Utc>)>,
}

impl WindowState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_or_create(
        &mut self,
        key: WindowKey,
        window_end: DateTime<Utc>,
        functions: &[(AggregateFunction, bool)],
    ) -> &mut (Vec<Accumulator>, DateTime<Utc>, DateTime<Utc>) {
        self.windows.entry(key.clone()).or_insert_with(|| {
            let accumulators = functions
                .iter()
                .map(|(f, distinct)| Accumulator::new(f.clone(), *distinct))
                .collect();
            (accumulators, key.window_start, window_end)
        })
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<
        Item = (
            &WindowKey,
            &(Vec<Accumulator>, DateTime<Utc>, DateTime<Utc>),
        ),
    > {
        self.windows.iter()
    }

    pub fn remove_expired(
        &mut self,
        cutoff: DateTime<Utc>,
    ) -> Vec<(WindowKey, Vec<Accumulator>, DateTime<Utc>, DateTime<Utc>)> {
        let expired_keys: Vec<_> = self
            .windows
            .iter()
            .filter(|(_, (_, _, end))| *end <= cutoff)
            .map(|(k, _)| k.clone())
            .collect();

        expired_keys
            .into_iter()
            .filter_map(|k| {
                self.windows
                    .remove(&k)
                    .map(|(acc, start, end)| (k, acc, start, end))
            })
            .collect()
    }

    pub fn len(&self) -> usize {
        self.windows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.windows.is_empty()
    }
}

/// Buffer for stream-stream WITHIN joins, keyed by timestamp.
#[derive(Debug, Default)]
pub struct JoinBuffer {
    buffer: BTreeMap<DateTime<Utc>, Vec<Row>>,
}

impl JoinBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, timestamp: DateTime<Utc>, row: Row) {
        self.buffer.entry(timestamp).or_default().push(row);
    }

    pub fn range(&self, from: DateTime<Utc>, to: DateTime<Utc>) -> impl Iterator<Item = &Row> {
        self.buffer
            .range(from..=to)
            .flat_map(|(_, rows)| rows.iter())
    }

    pub fn expire_before(&mut self, cutoff: DateTime<Utc>) {
        let to_remove: Vec<_> = self.buffer.range(..cutoff).map(|(k, _)| *k).collect();
        for k in to_remove {
            self.buffer.remove(&k);
        }
    }

    pub fn len(&self) -> usize {
        self.buffer.values().map(|v| v.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}
