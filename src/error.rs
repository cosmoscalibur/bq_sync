//! Crate-wide error type.

use std::path::PathBuf;

/// Errors produced by bq-sync operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A BigQuery / GCP resource does not exist (HTTP 404).
    #[error("{0}")]
    NotFound(String),

    /// A GCP REST call failed with a non-404 status.
    #[error("GCP API error (HTTP {status}): {message}")]
    Api { status: u16, message: String },

    /// Invalid input or a missing remote resource looked up by name.
    #[error("{0}")]
    Invalid(String),

    /// Configuration file missing or malformed.
    #[error("{0}")]
    Config(String),

    /// I/O failure, annotated with the path involved.
    #[error("{path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// Fatal condition already reported to the user; terminate with this
    /// exit status (the equivalent of Python's ``sys.exit(code)``).
    #[error("exit status {0}")]
    Exit(i32),

    /// Any other failure (authentication, HTTP transport, serialization …).
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl Error {
    /// Build an [`Error::Io`] for *path*.
    pub fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Error::Io {
            path: path.into(),
            source,
        }
    }

    /// Whether this error represents a missing remote resource.
    pub fn is_not_found(&self) -> bool {
        matches!(self, Error::NotFound(_))
    }
}

/// Crate-wide result alias.
pub type Result<T, E = Error> = std::result::Result<T, E>;
