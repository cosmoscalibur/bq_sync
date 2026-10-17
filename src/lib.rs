//! # bq-sync
//!
//! Sync BigQuery resources (views, routines, table models, external tables,
//! scheduled queries and saved queries) to a local directory tree, push
//! local edits back, validate SQL offline, and move table data in and out.
//!
//! The [`cli`] module implements the `bq-sync` command; every subcommand is
//! also available as a library function taking a [`api::BqApi`].

pub mod api;
pub mod cli;
pub mod config;
pub mod data;
pub mod error;
pub mod fetch;
pub mod logging;
pub mod materialize;
pub mod pull;
pub mod push;
pub mod readers;
pub mod reschedule;
pub mod resources;
pub mod sql_check;
pub mod utils;
pub mod writers;

#[cfg(feature = "python")]
mod python;
#[cfg(test)]
mod testing;

pub use error::{Error, Result};

/// Crate version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
