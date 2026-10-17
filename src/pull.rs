//! Pull orchestrator — fetch BigQuery resources to the local filesystem.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};

use crate::api::BqApi;
use crate::config::{SyncConfig, resolve_output_dir};
use crate::error::{Error, Result};
use crate::fetch::{FetchAction, has_uncommitted_changes, resolve};
use crate::resources::DatasetResources;
use crate::utils::file_stem_of;
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

/// Write the views, routines, tables and external tables of one dataset.
fn write_dataset(
    output_root: &Path,
    dataset: &str,
    resources: &DatasetResources,
    opts: &PullOptions,
) -> Result<()> {
    let ds_dir = output_root.join(dataset);
    let models = |name: &str| ds_dir.join("models").join(format!("{name}.yaml"));

    for view in &resources.views {
        let sql_path = ds_dir.join("views").join(format!("{}.sql", view.name));
        if opts.should_fetch("view", &view.name, view.modified, &sql_path) {
            writers::write_view_sql(&sql_path, view)?;
            writers::write_view_model_yaml(&models(&view.name), view)?;
        }
    }

    for routine in &resources.routines {
        let sql_path = ds_dir
            .join("routines")
            .join(format!("{}.sql", routine.name));
        if opts.should_fetch("routine", &routine.name, routine.modified, &sql_path) {
            writers::write_routine_sql(&sql_path, routine)?;
            writers::write_routine_model_yaml(&models(&routine.name), routine)?;
        }
    }

    for table in &resources.tables {
        let path = models(&table.name);
        if opts.should_fetch("model", &table.name, table.modified, &path) {
            writers::write_model_yaml(&path, table)?;
        }
    }

    for ext in &resources.externals {
        let path = models(&ext.name);
        if opts.should_fetch("external", &ext.name, ext.modified, &path) {
            writers::write_external_definition(&path, ext)?;
        }
    }
    Ok(())
}

/// File for the project-level query *name* in *dir*.  ``None`` when an
/// earlier query of the same pull already maps to that file name, so the
/// first one is not overwritten.
fn query_path(
    dir: &Path,
    name: &str,
    kind: &str,
    claimed: &mut HashSet<PathBuf>,
) -> Option<PathBuf> {
    if name.contains(['\n', '\r']) {
        log::warn!(
            "Skipping {kind} '{}': a name with a line break cannot be stored in its file header.",
            name.escape_debug()
        );
        return None;
    }
    let path = dir.join(format!("{}.sql", file_stem_of(name)));
    // Case-insensitive filesystems map names differing only in case to one file.
    if claimed.insert(PathBuf::from(path.to_string_lossy().to_lowercase())) {
        Some(path)
    } else {
        log::warn!(
            "Skipping {kind} '{name}': {} already holds another {kind}.",
            path.display()
        );
        None
    }
}

/// Write the project-level *queries* of one *kind* into *dir*; *describe*
/// gives a query's name and modification time, *write* writes its file.
fn write_queries<T>(
    dir: &Path,
    kind: &str,
    queries: &[T],
    opts: &PullOptions,
    describe: impl Fn(&T) -> (&str, DateTime<Utc>),
    write: impl Fn(&Path, &T) -> Result<()>,
) -> Result<()> {
    let mut claimed = HashSet::new();
    for query in queries {
        let (name, modified) = describe(query);
        let Some(path) = query_path(dir, name, kind, &mut claimed) else {
            continue;
        };
        if opts.should_fetch(kind, name, modified, &path) {
            write(&path, query)?;
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
    let listing = api.list_pull_resources(
        &config.project.id,
        &config.project.default_region,
        &config.datasets,
    )?;
    for (dataset, resources) in config.datasets.iter().zip(&listing.datasets) {
        log::info!("Processing dataset '{dataset}'");
        write_dataset(&output_root, dataset, resources, opts)?;
    }
    write_queries(
        &output_root.join("scheduled_queries"),
        "scheduled query",
        &listing.scheduled,
        opts,
        |q| (q.name.as_str(), q.modified),
        writers::write_scheduled_query_sql,
    )?;
    write_queries(
        &output_root.join("saved_queries"),
        "saved query",
        &listing.saved,
        opts,
        |q| (q.name.as_str(), q.modified),
        writers::write_saved_query_sql,
    )?;

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
    fn project_query_names_stay_inside_their_directory() {
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("bq_sync.toml");
        let api = fake();
        let ts = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let scheduled = |name: &str| ScheduledQueryInfo {
            name: name.into(),
            sql: "SELECT 1".into(),
            schedule: "every 24 hours".into(),
            modified: ts,
        };
        *api.scheduled.borrow_mut() = vec![
            scheduled("../../escape"),
            scheduled("a/b"),
            scheduled("a_b"),
        ];
        let root = pull_project(&api, &config(), &cfg_path, &PullOptions::default()).unwrap();
        let written: Vec<_> = std::fs::read_dir(root.join("scheduled_queries"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(written.len(), 2, "{written:?}");
        assert!(written.contains(&".._.._escape.sql".to_owned()));
        assert!(!dir.path().join("escape.sql").exists());
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
