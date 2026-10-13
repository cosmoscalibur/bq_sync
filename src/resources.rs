//! Plain data types describing BigQuery resources.

use chrono::{DateTime, Utc};

/// Unix epoch, used when BigQuery reports no modification time.
pub const EPOCH: DateTime<Utc> = DateTime::UNIX_EPOCH;

/// A column of a table / view schema.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SchemaField {
    pub name: String,
    pub field_type: String,
    pub mode: String,
    pub description: String,
}

impl SchemaField {
    pub fn new(name: &str, field_type: &str, mode: &str, description: &str) -> Self {
        SchemaField {
            name: name.into(),
            field_type: field_type.into(),
            mode: mode.into(),
            description: description.into(),
        }
    }
}

/// An argument of a routine signature.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutineArgument {
    pub name: String,
    pub arg_type: String,
    pub mode: String,
}

impl Default for RoutineArgument {
    fn default() -> Self {
        RoutineArgument {
            name: String::new(),
            arg_type: "ANY".into(),
            mode: "IN".into(),
        }
    }
}

/// A BigQuery view definition.
#[derive(Debug, Clone, PartialEq)]
pub struct ViewInfo {
    pub name: String,
    pub sql: String,
    pub modified: DateTime<Utc>,
    pub schema: Vec<SchemaField>,
    pub description: String,
    pub created: Option<DateTime<Utc>>,
    pub region: Option<String>,
}

/// A BigQuery routine (function / procedure).
#[derive(Debug, Clone, PartialEq)]
pub struct RoutineInfo {
    pub name: String,
    pub sql: String,
    pub language: String,
    pub modified: DateTime<Utc>,
    pub description: String,
    pub created: Option<DateTime<Utc>>,
    pub arguments: Vec<RoutineArgument>,
    pub return_type: Option<String>,
}

/// BigQuery table metadata (model export).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TableInfo {
    pub name: String,
    pub schema: Vec<SchemaField>,
    pub description: String,
    pub row_count: i64,
    pub modified: DateTime<Utc>,
    pub partitioning: Option<String>,
    pub clustering: Option<Vec<String>>,
    pub created: Option<DateTime<Utc>>,
    pub region: Option<String>,
    pub primary_keys: Option<Vec<String>>,
    pub total_logical_bytes: Option<i64>,
}

/// A BigQuery external table definition.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ExternalTableInfo {
    pub name: String,
    pub source_uris: Vec<String>,
    pub schema: Vec<SchemaField>,
    pub source_format: String,
    pub modified: DateTime<Utc>,
    pub description: String,
    pub created: Option<DateTime<Utc>>,
    pub region: Option<String>,
    pub total_logical_bytes: Option<i64>,
    pub row_count: i64,
    pub partitioning: Option<String>,
    pub clustering: Option<Vec<String>>,
    pub primary_keys: Option<Vec<String>>,
}

/// A BigQuery scheduled query.
#[derive(Debug, Clone, PartialEq)]
pub struct ScheduledQueryInfo {
    pub name: String,
    pub sql: String,
    pub schedule: String,
    pub modified: DateTime<Utc>,
}

/// A BigQuery Studio saved query (Dataform-backed).
#[derive(Debug, Clone, PartialEq)]
pub struct SavedQueryInfo {
    pub name: String,
    pub sql: String,
    pub modified: DateTime<Utc>,
}

/// A Data Transfer Service config for a scheduled query.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TransferConfig {
    /// Full resource name (``projects/…/locations/…/transferConfigs/…``).
    pub name: String,
    pub display_name: String,
    pub schedule: String,
    pub data_source_id: String,
    pub query: String,
    pub update_time: Option<DateTime<Utc>>,
}

/// Tabular output format for data fetch / load.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataFormat {
    Csv,
    Parquet,
}

impl DataFormat {
    pub fn extension(self) -> &'static str {
        match self {
            DataFormat::Csv => "csv",
            DataFormat::Parquet => "parquet",
        }
    }

    pub fn parse(s: &str) -> crate::Result<Self> {
        match s {
            "csv" => Ok(DataFormat::Csv),
            "parquet" => Ok(DataFormat::Parquet),
            other => Err(crate::Error::Invalid(format!(
                "Unsupported format: '{other}'. Expected 'csv' or 'parquet'."
            ))),
        }
    }
}
