//! Pull orchestrator — fetch BigQuery resources to the local filesystem.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};

use crate::api::BqApi;
use crate::config::{SyncConfig, resolve_output_dir};
use crate::error::{Error, Result};
use crate::fetch::{FetchAction, has_uncommitted_changes, resolve};
use crate::writers;

/// Options shared by the pull functions.
#[derive(Debug, Clone, Default)]
pub struct PullOptions {
    /// Log actions without writing files.
    pub dry_run: bool,
    /// Bypass the fetch decision matrix.
    pub force: bool,
    /// When set, ``force`` only applies to these files (path or file name).
    pub force_files: Option<Vec<String>>,
}

impl PullOptions {
    fn is_forced(&self, file: &Path) -> bool {
        if !self.force {
            return false;
        }
        match &self.force_files {
            None => true,
            Some(list) => {
                let full = file.to_string_lossy();
                let name = file
                    .file_name()
                    .map(|n| n.to_string_lossy())
                    .unwrap_or_default();
                list.iter().any(|f| *f == full || *f == name)
            }
        }
    }

    /// Decide whether *path* should be (re)written; logs the outcome.
    fn should_fetch(&self, kind: &str, name: &str, modified: DateTime<Utc>, path: &Path) -> bool {
        match resolve(Some(modified), path, self.is_forced(path)) {
            FetchAction::Skip => {
                log::debug!("SKIP {kind} {name}");
                false
            }
            FetchAction::Warn => false,
            FetchAction::Fetch => {
                log::info!("FETCH {kind} {name} -> {}", path.display());
                !self.dry_run
            }
        }
    }
}

/// Pull views, routines, tables and external tables of one dataset.
pub fn pull_dataset(
    api: &dyn BqApi,
    config: &SyncConfig,
    output_root: &Path,
    dataset: &str,
    opts: &PullOptions,
) -> Result<()> {
    let project = &config.project.id;
    let ds_dir = output_root.join(dataset);
    let models = |name: &str| ds_dir.join("models").join(format!("{name}.yaml"));

    for view in api.list_views(project, dataset)? {
        let sql_path = ds_dir.join("views").join(format!("{}.sql", view.name));
        if opts.should_fetch("view", &view.name, view.modified, &sql_path) {
            writers::write_view_sql(&sql_path, &view)?;
            writers::write_view_model_yaml(&models(&view.name), &view)?;
        }
    }

    for routine in api.list_routines(project, dataset)? {
        let sql_path = ds_dir
            .join("routines")
            .join(format!("{}.sql", routine.name));
        if opts.should_fetch("routine", &routine.name, routine.modified, &sql_path) {
            writers::write_routine_sql(&sql_path, &routine)?;
            writers::write_routine_model_yaml(&models(&routine.name), &routine)?;
        }
    }

    for table in api.list_tables(project, dataset)? {
        let path = models(&table.name);
        if opts.should_fetch("model", &table.name, table.modified, &path) {
            writers::write_model_yaml(&path, &table)?;
        }
    }

    for ext in api.list_external_tables(project, dataset)? {
        let path = models(&ext.name);
        if opts.should_fetch("external", &ext.name, ext.modified, &path) {
            writers::write_external_definition(&path, &ext)?;
        }
    }
    Ok(())
}

/// Pull project-level scheduled queries.
pub fn pull_scheduled_queries(
    api: &dyn BqApi,
    config: &SyncConfig,
    output_root: &Path,
    opts: &PullOptions,
) -> Result<()> {
    let dir = output_root.join("scheduled_queries");
    for sq in api.list_scheduled_queries(&config.project.id, &config.project.default_region)? {
        let path = dir.join(format!("{}.sql", sq.name));
        if opts.should_fetch("scheduled query", &sq.name, sq.modified, &path) {
            writers::write_scheduled_query_sql(&path, &sq)?;
        }
    }
    Ok(())
}

/// Pull project-level saved queries (Dataform).
pub fn pull_saved_queries(
    api: &dyn BqApi,
    config: &SyncConfig,
    output_root: &Path,
    opts: &PullOptions,
) -> Result<()> {
    let dir = output_root.join("saved_queries");
    for saved in api.list_saved_queries(&config.project.id, &config.project.default_region)? {
        let path = dir.join(format!("{}.sql", saved.name));
        if opts.should_fetch("saved query", &saved.name, saved.modified, &path) {
            writers::write_saved_query_sql(&path, &saved)?;
        }
    }
    Ok(())
}

/// Pull every configured resource of the project.
///
/// Refuses to run when the output directory has uncommitted changes.
pub fn pull_project(
    api: &dyn BqApi,
    config: &SyncConfig,
    config_path: &Path,
    opts: &PullOptions,
) -> Result<PathBuf> {
    let output_root = resolve_output_dir(config, config_path);

    if output_root.exists() && has_uncommitted_changes(&output_root) {
        log::error!(
            "Uncommitted changes detected in {}. \
             Commit or stash changes before running bq-sync pull.",
            output_root.display()
        );
        return Err(Error::Exit(1));
    }

    log::info!(
        "Pulling project '{}' to {}",
        config.project.id,
        output_root.display()
    );
    for dataset in &config.datasets {
        log::info!("Processing dataset '{dataset}'");
        pull_dataset(api, config, &output_root, dataset, opts)?;
    }
    pull_scheduled_queries(api, config, &output_root, opts)?;
    pull_saved_queries(api, config, &output_root, opts)?;

    log::info!("Pull complete.");
    log::info!("Hint: review changes with 'git diff' and commit with 'git add -A && git commit'.");
    Ok(output_root)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::{
        ExternalTableInfo, RoutineInfo, SavedQueryInfo, ScheduledQueryInfo, TableInfo, ViewInfo,
    };
    use crate::testing::FakeApi;
    use chrono::TimeZone;

    fn config() -> SyncConfig {
        SyncConfig::from_toml(
            "[project]\nid = \"my-project\"\ndefault_region = \"us-east1\"\n[sync]\ndatasets = [\"ds\"]\n",
        )
        .unwrap()
    }

    fn fake() -> FakeApi {
        let ts = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let fake = FakeApi::default();
        *fake.views.borrow_mut() = vec![ViewInfo {
            name: "v".into(),
            sql: "SELECT 1".into(),
            modified: ts,
            schema: vec![],
            description: String::new(),
            created: None,
            region: None,
        }];
        *fake.routines.borrow_mut() = vec![RoutineInfo {
            name: "fn".into(),
            sql: "1".into(),
            language: "SQL".into(),
            modified: ts,
            description: String::new(),
            created: None,
            arguments: vec![],
            return_type: None,
        }];
        *fake.tables.borrow_mut() = vec![TableInfo {
            name: "t".into(),
            modified: ts,
            ..Default::default()
        }];
        *fake.externals.borrow_mut() = vec![ExternalTableInfo {
            name: "e".into(),
            modified: ts,
            ..Default::default()
        }];
        *fake.scheduled.borrow_mut() = vec![ScheduledQueryInfo {
            name: "daily".into(),
            sql: "SELECT 2".into(),
            schedule: "every 24 hours".into(),
            modified: ts,
        }];
        *fake.saved.borrow_mut() = vec![SavedQueryInfo {
            name: "sq".into(),
            sql: "SELECT 3".into(),
            modified: ts,
        }];
        fake
    }

    #[test]
    fn pulls_all_resource_kinds() {
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("bq_sync.toml");
        let api = fake();
        let root = pull_project(&api, &config(), &cfg_path, &PullOptions::default()).unwrap();
        for rel in [
            "ds/views/v.sql",
            "ds/models/v.yaml",
            "ds/routines/fn.sql",
            "ds/models/fn.yaml",
            "ds/models/t.yaml",
            "ds/models/e.yaml",
            "scheduled_queries/daily.sql",
            "saved_queries/sq.sql",
        ] {
            assert!(root.join(rel).is_file(), "missing {rel}");
        }
    }

    #[test]
    fn dry_run_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("bq_sync.toml");
        let opts = PullOptions {
            dry_run: true,
            ..Default::default()
        };
        let root = pull_project(&fake(), &config(), &cfg_path, &opts).unwrap();
        assert!(!root.exists());
    }

    #[test]
    fn force_file_filter() {
        let opts = PullOptions {
            force: true,
            force_files: Some(vec!["v.sql".into()]),
            ..Default::default()
        };
        assert!(opts.is_forced(Path::new("/x/views/v.sql")));
        assert!(!opts.is_forced(Path::new("/x/views/w.sql")));
    }
}
