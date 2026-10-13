//! Access to BigQuery, BigQuery Data Transfer and Dataform.
//!
//! Orchestrators depend on the [`BqApi`] trait so they can be exercised
//! against in-memory fakes; [`GcpClient`] is the production implementation
//! backed by the official Google Cloud Rust SDK.

mod gcp;
pub mod tabular;

use std::collections::HashMap;
use std::path::Path;

pub use gcp::GcpClient;

use crate::error::Result;
use crate::resources::{
    DataFormat, ExternalTableInfo, RoutineArgument, RoutineInfo, SavedQueryInfo,
    ScheduledQueryInfo, TableInfo, TransferConfig, ViewInfo,
};

/// Operations bq-sync performs against Google Cloud.
pub trait BqApi {
    // ----- read (pull) -----
    fn list_views(&self, project: &str, dataset: &str) -> Result<Vec<ViewInfo>>;
    fn list_routines(&self, project: &str, dataset: &str) -> Result<Vec<RoutineInfo>>;
    fn list_tables(&self, project: &str, dataset: &str) -> Result<Vec<TableInfo>>;
    fn list_external_tables(&self, project: &str, dataset: &str) -> Result<Vec<ExternalTableInfo>>;
    fn get_table_info(&self, project: &str, dataset: &str, name: &str) -> Result<TableInfo>;

    /// Saved queries via Dataform.  Returns an empty list (with a warning)
    /// when Dataform is unavailable.
    fn list_saved_queries(&self, project: &str, region: &str) -> Result<Vec<SavedQueryInfo>>;

    // ----- scheduled queries (Data Transfer) -----
    /// Transfer configs with ``data_source_id == "scheduled_query"``.
    fn list_transfer_configs(&self, project: &str, region: &str) -> Result<Vec<TransferConfig>>;

    /// Patch the ``schedule`` of a transfer config (by resource name).
    fn update_transfer_config_schedule(
        &self,
        project: &str,
        name: &str,
        schedule: &str,
    ) -> Result<()>;

    /// Start a manual run, now, for a transfer config (by resource name).
    fn start_manual_transfer_run(&self, project: &str, name: &str) -> Result<()>;

    /// Replace ``params.query`` of a transfer config (by resource name),
    /// keeping its other parameters.
    fn update_transfer_config_query(&self, project: &str, name: &str, sql: &str) -> Result<()>;

    fn list_scheduled_queries(
        &self,
        project: &str,
        region: &str,
    ) -> Result<Vec<ScheduledQueryInfo>> {
        Ok(self
            .list_transfer_configs(project, region)?
            .into_iter()
            .map(|tc| ScheduledQueryInfo {
                name: tc.display_name,
                sql: tc.query,
                schedule: tc.schedule,
                modified: tc.update_time.unwrap_or(crate::resources::EPOCH),
            })
            .collect())
    }

    /// First scheduled query whose display name matches exactly.
    fn get_transfer_config(
        &self,
        project: &str,
        region: &str,
        display_name: &str,
    ) -> Result<Option<TransferConfig>> {
        Ok(self
            .list_transfer_configs(project, region)?
            .into_iter()
            .find(|tc| tc.display_name == display_name))
    }

    /// Update the schedule of the scheduled query named *display_name*.
    fn update_transfer_schedule(
        &self,
        project: &str,
        region: &str,
        display_name: &str,
        schedule: &str,
    ) -> Result<()> {
        let tc = self
            .get_transfer_config(project, region, display_name)?
            .ok_or_else(|| sq_not_found(project, region, display_name))?;
        self.update_transfer_config_schedule(project, &tc.name, schedule)?;
        log::info!("Updated schedule for '{display_name}' to '{schedule}'.");
        Ok(())
    }

    /// Update the SQL of the scheduled query named *display_name*.
    ///
    /// There is no create path: a scheduled query also needs a schedule,
    /// destination and other settings the SQL file does not carry, so a
    /// missing query is reported and skipped.
    fn upsert_scheduled_query_sql(
        &self,
        project: &str,
        region: &str,
        display_name: &str,
        sql: &str,
    ) -> Result<()> {
        let Some(tc) = self.get_transfer_config(project, region, display_name)? else {
            log::warn!(
                "Scheduled query '{display_name}' not found in {project}/{region}; \
                 skipping SQL push. Create the scheduled query in the BQ console first."
            );
            return Ok(());
        };
        self.update_transfer_config_query(project, &tc.name, sql)?;
        log::info!("Updated SQL for scheduled query '{display_name}'.");
        Ok(())
    }

    /// Trigger an immediate manual run of the scheduled query.
    fn trigger_transfer_run(&self, project: &str, region: &str, display_name: &str) -> Result<()> {
        let tc = self
            .get_transfer_config(project, region, display_name)?
            .ok_or_else(|| sq_not_found(project, region, display_name))?;
        self.start_manual_transfer_run(project, &tc.name)?;
        log::info!("Triggered manual run for '{display_name}'.");
        Ok(())
    }

    // ----- data -----
    fn fetch_table_to_file(
        &self,
        project: &str,
        dataset: &str,
        table: &str,
        dest: &Path,
        fmt: DataFormat,
    ) -> Result<()>;
    fn fetch_query_to_file(
        &self,
        project: &str,
        sql: &str,
        dest: &Path,
        fmt: DataFormat,
    ) -> Result<()>;
    fn load_table_from_file(
        &self,
        project: &str,
        dataset: &str,
        table: &str,
        source: &Path,
        fmt: DataFormat,
    ) -> Result<()>;

    // ----- write (push) -----
    fn upsert_view(&self, project: &str, dataset: &str, name: &str, sql: &str) -> Result<()>;
    fn upsert_table_description(
        &self,
        project: &str,
        dataset: &str,
        name: &str,
        description: &str,
        field_descriptions: &HashMap<String, String>,
    ) -> Result<()>;
    #[allow(clippy::too_many_arguments)]
    fn upsert_routine(
        &self,
        project: &str,
        dataset: &str,
        name: &str,
        body: &str,
        language: &str,
        arguments: Option<&[RoutineArgument]>,
        return_type: Option<&str>,
    ) -> Result<()>;
    fn upsert_routine_description(
        &self,
        project: &str,
        dataset: &str,
        name: &str,
        description: &str,
    ) -> Result<()>;
    fn upsert_saved_query(&self, project: &str, region: &str, name: &str, sql: &str) -> Result<()>;

    // ----- delete (rm) -----
    fn delete_view(&self, project: &str, dataset: &str, name: &str) -> Result<()>;
    fn delete_table(&self, project: &str, dataset: &str, name: &str) -> Result<()>;
    fn delete_routine(&self, project: &str, dataset: &str, name: &str) -> Result<()>;
    fn delete_saved_query(&self, project: &str, region: &str, name: &str) -> Result<()>;

    // ----- queries -----
    /// Run a statement (DDL/DML) and wait for completion.
    fn run_query(&self, project: &str, sql: &str, location: Option<&str>) -> Result<()>;
    /// Validate a query with a BigQuery dry run.
    fn dry_run_query(&self, project: &str, sql: &str) -> Result<()>;
}

fn sq_not_found(project: &str, region: &str, display_name: &str) -> crate::Error {
    crate::Error::Invalid(format!(
        "Scheduled query '{display_name}' not found in {project}/{region}."
    ))
}
