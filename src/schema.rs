use std::collections::HashMap;

use anyhow::{Result, bail};

use crate::types::{Column, DataType, Schema};

/// Whether a registered source is a stream or a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceType {
    Stream,
    Table,
}

/// Metadata for a registered source (stream or table).
#[derive(Debug, Clone)]
pub struct SourceInfo {
    pub name: String,
    pub source_type: SourceType,
    pub schema: Schema,
    pub topic: String,
    pub key_column: Option<String>,
    pub value_format: String,
}

/// Registry of known streams and tables, mapping names to their schemas and topics.
#[derive(Debug, Default)]
pub struct SchemaRegistry {
    sources: HashMap<String, SourceInfo>,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_stream(
        &mut self,
        name: impl Into<String>,
        schema: Schema,
        topic: impl Into<String>,
    ) -> Result<()> {
        let name = name.into();
        let key = name.to_uppercase();
        if self.sources.contains_key(&key) {
            bail!("Source '{}' already registered", name);
        }
        self.sources.insert(
            key,
            SourceInfo {
                name,
                source_type: SourceType::Stream,
                schema,
                topic: topic.into(),
                key_column: None,
                value_format: "JSON".to_string(),
            },
        );
        Ok(())
    }

    pub fn register_table(
        &mut self,
        name: impl Into<String>,
        schema: Schema,
        topic: impl Into<String>,
        key_column: impl Into<String>,
    ) -> Result<()> {
        let name = name.into();
        let key = name.to_uppercase();
        if self.sources.contains_key(&key) {
            bail!("Source '{}' already registered", name);
        }
        self.sources.insert(
            key,
            SourceInfo {
                name,
                source_type: SourceType::Table,
                schema,
                topic: topic.into(),
                key_column: Some(key_column.into()),
                value_format: "JSON".to_string(),
            },
        );
        Ok(())
    }

    pub fn register_source(&mut self, info: SourceInfo) -> Result<()> {
        let key = info.name.to_uppercase();
        if self.sources.contains_key(&key) {
            bail!("Source '{}' already registered", info.name);
        }
        self.sources.insert(key, info);
        Ok(())
    }

    pub fn get(&self, name: &str) -> Option<&SourceInfo> {
        self.sources.get(&name.to_uppercase())
    }

    pub fn remove(&mut self, name: &str) -> Option<SourceInfo> {
        self.sources.remove(&name.to_uppercase())
    }

    pub fn list(&self) -> Vec<&SourceInfo> {
        self.sources.values().collect()
    }

    pub fn schema_for(&self, name: &str) -> Option<&Schema> {
        self.get(name).map(|s| &s.schema)
    }

    pub fn topic_for(&self, name: &str) -> Option<&str> {
        self.get(name).map(|s| s.topic.as_str())
    }

    /// Build a Schema from column definitions, used by SQL parser.
    pub fn build_schema(columns: &[(String, DataType)]) -> Schema {
        Schema::new(
            columns
                .iter()
                .map(|(name, dt)| Column::new(name.clone(), dt.clone()))
                .collect(),
        )
    }
}
