use std::collections::HashMap;

use anyhow::Result;
use merkql::broker::BrokerRef;

use crate::plan::QueryPlan;
use crate::runtime::persistent::{PersistentQuery, QueryStatus};
use crate::schema::SchemaRegistry;

/// Registry of all running persistent queries.
pub struct QueryRegistry {
    queries: HashMap<String, PersistentQuery>,
    next_id: u64,
}

impl QueryRegistry {
    pub fn new() -> Self {
        Self {
            queries: HashMap::new(),
            next_id: 1,
        }
    }

    /// Start a new persistent query and register it.
    pub fn start_query(
        &mut self,
        plan: QueryPlan,
        sink_topic: String,
        broker: &BrokerRef,
        registry: &SchemaRegistry,
    ) -> Result<String> {
        let id = format!("q{}", self.next_id);
        self.next_id += 1;

        let query = PersistentQuery::start(id.clone(), plan, sink_topic, broker.clone(), registry)?;

        self.queries.insert(id.clone(), query);
        Ok(id)
    }

    /// List all query IDs and their statuses.
    pub fn list(&self) -> Vec<(String, QueryStatus)> {
        self.queries
            .iter()
            .map(|(id, q)| (id.clone(), q.status()))
            .collect()
    }

    /// Get the status of a specific query.
    pub fn status(&self, id: &str) -> Option<QueryStatus> {
        self.queries.get(id).map(|q| q.status())
    }

    /// Stop a query gracefully.
    pub fn stop(&mut self, id: &str) -> Result<()> {
        let query = self
            .queries
            .get_mut(id)
            .ok_or_else(|| anyhow::anyhow!("Query not found: {}", id))?;
        query.stop();
        Ok(())
    }

    /// Terminate a query immediately.
    pub fn terminate(&mut self, id: &str) -> Result<()> {
        let query = self
            .queries
            .get_mut(id)
            .ok_or_else(|| anyhow::anyhow!("Query not found: {}", id))?;
        query.terminate();
        Ok(())
    }

    /// Stop all running queries.
    pub fn stop_all(&mut self) {
        for query in self.queries.values_mut() {
            if query.status() == QueryStatus::Running {
                query.stop();
            }
        }
    }

    /// Number of registered queries.
    pub fn len(&self) -> usize {
        self.queries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queries.is_empty()
    }
}

impl Default for QueryRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for QueryRegistry {
    fn drop(&mut self) {
        self.stop_all();
    }
}
