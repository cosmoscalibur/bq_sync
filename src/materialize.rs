//! Materialize a view or external table into a permanent table.
//!
//! The resource may be named by either side of the pair (``view_x`` /
//! ``external_x`` or ``materialize_x``).  When a scheduled query named after
//! the target exists, that scheduled query is run, keeping the
//! customization in its own DDL (``CLUSTER BY``, partitioning, …).
//! Otherwise ``CREATE OR REPLACE TABLE <target> AS SELECT * FROM <source>``
//! runs.  Either way the new table's model YAML is written so the local
//! tree stays in sync.

use std::path::{Path, PathBuf};

use crate::api::BqApi;
use crate::config::{SyncConfig, resolve_output_dir};
use crate::error::{Error, Result};
use crate::utils::{confirm, parse_resource_path};
use crate::writers::write_model_yaml;

const RENAME_PREFIXES: [&str; 2] = ["view_", "external_"];

/// Default target name: a leading ``view_`` / ``external_`` prefix becomes
/// ``materialize_``; other names are kept unchanged.
pub fn default_target_name(source_name: &str) -> String {
    RENAME_PREFIXES
        .iter()
        .find_map(|p| source_name.strip_prefix(p))
        .map(|rest| format!("materialize_{rest}"))
        .unwrap_or_else(|| source_name.to_owned())
}

const MATERIALIZE_PREFIX: &str = "materialize_";

/// Names of the source candidates for a ``materialize_x`` target.
fn source_candidates(target: &str) -> Vec<String> {
    target
        .strip_prefix(MATERIALIZE_PREFIX)
        .map(|rest| {
            RENAME_PREFIXES
                .iter()
                .map(|p| format!("{p}{rest}"))
                .collect()
        })
        .unwrap_or_default()
}

/// DDL that (re)creates *target_name* from *source_name*.
pub fn build_ddl(project: &str, dataset: &str, source_name: &str, target_name: &str) -> String {
    format!(
        "CREATE OR REPLACE TABLE `{project}.{dataset}.{target_name}`\n\
         AS SELECT * FROM `{project}.{dataset}.{source_name}`"
    )
}

/// The existing view or external table that *target* is materialized from.
fn resolve_source(api: &dyn BqApi, project: &str, dataset: &str, target: &str) -> Result<String> {
    let candidates = source_candidates(target);
    let views = api.list_views(project, dataset)?;
    let externals = api.list_external_tables(project, dataset)?;
    candidates
        .into_iter()
        .find(|c| views.iter().any(|v| &v.name == c) || externals.iter().any(|e| &e.name == c))
        .ok_or_else(|| {
            Error::Invalid(format!(
                "No scheduled query '{target}' and no view or external table to \
                 materialize it from in {project}.{dataset}."
            ))
        })
}

/// Options for [`materialize_resource`].
#[derive(Debug, Clone, Default)]
pub struct MaterializeOptions {
    pub dry_run: bool,
    pub yes: bool,
    /// Override the target table name.
    pub target_name: Option<String>,
}

/// Materialize *resource_path* (``<project>/<dataset>/[<dir>/]<name>``).
///
/// Returns the written model YAML path, or ``None`` when nothing ran.
pub fn materialize_resource(
    api: &dyn BqApi,
    config: &SyncConfig,
    config_path: &Path,
    resource_path: &str,
    opts: &MaterializeOptions,
) -> Result<Option<PathBuf>> {
    let output_root = resolve_output_dir(config, config_path);
    let location = &config.project.default_region;
    let (project, dataset, given) = parse_resource_path(resource_path)?;
    // Either pattern identifies the pair: ``view_x`` / ``external_x`` map to
    // ``materialize_x``, and ``materialize_x`` is its own target.
    let given_is_target = given.starts_with(MATERIALIZE_PREFIX);
    let target = opts
        .target_name
        .clone()
        .filter(|t| !t.is_empty())
        .unwrap_or_else(|| {
            if given_is_target {
                given.clone()
            } else {
                default_target_name(&given)
            }
        });
    // The scheduled query that refreshes the target is named after it (the
    // ``-- Scheduled Query:`` header of its file); running it keeps the
    // materialization's own DDL, which the generic one would drop.
    let scheduled = api.get_transfer_config(&project, location, &target)?;
    let source = if given_is_target {
        match &scheduled {
            Some(_) => None,
            None => Some(resolve_source(api, &project, &dataset, &given)?),
        }
    } else {
        Some(given)
    };
    let ddl = source
        .as_deref()
        .map(|s| build_ddl(&project, &dataset, s, &target));

    println!();
    if let Some(s) = &source {
        println!("  Source : {project}.{dataset}.{s}");
    }
    println!("  Target : {project}.{dataset}.{target}");
    match (&scheduled, &ddl) {
        (Some(tc), _) => println!(
            "\n  Scheduled query '{}' will be run instead of the generic DDL:\n\n    {}\n",
            tc.display_name,
            tc.query.trim().replace('\n', "\n    ")
        ),
        (None, Some(ddl)) => println!("\n  DDL:\n\n    {}\n", ddl.replace('\n', "\n    ")),
        (None, None) => unreachable!("a missing scheduled query always yields a source"),
    }

    if opts.dry_run {
        log::info!("Dry-run: no changes written.");
        return Ok(None);
    }
    if !opts.yes && !confirm("Proceed with materialization?") {
        log::info!("Materialization cancelled.");
        return Ok(None);
    }

    log::info!("Materializing {project}.{dataset}.{target} …");
    match (&scheduled, &ddl) {
        (Some(tc), _) => api.run_transfer_config_and_wait(&project, &tc.name)?,
        (None, Some(ddl)) => api.run_query(&project, ddl, Some(location))?,
        (None, None) => unreachable!("a missing scheduled query always yields a source"),
    }
    log::info!("Materialization complete.");

    log::info!("Pulling model YAML for '{target}' …");
    let table = api.get_table_info(&project, &dataset, &target)?;
    let model_path = output_root
        .join(&dataset)
        .join("models")
        .join(format!("{target}.yaml"));
    write_model_yaml(&model_path, &table)?;
    log::info!("Wrote model YAML to {}", model_path.display());
    log::info!("Hint: review with 'git diff' and commit with 'git add -A && git commit'.");
    Ok(Some(model_path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::FakeApi;

    #[test]
    fn target_names() {
        assert_eq!(
            default_target_name("view_auth_user_valid"),
            "materialize_auth_user_valid"
        );
        assert_eq!(default_target_name("external_nps"), "materialize_nps");
        assert_eq!(default_target_name("snapshot_events"), "snapshot_events");
        assert_eq!(default_target_name("view_"), "materialize_");
    }

    #[test]
    fn ddl() {
        assert_eq!(
            build_ddl("p", "d", "view_x", "materialize_x"),
            "CREATE OR REPLACE TABLE `p.d.materialize_x`\nAS SELECT * FROM `p.d.view_x`"
        );
    }

    fn config() -> SyncConfig {
        SyncConfig::from_toml(
            "[project]\nid = \"proj\"\ndefault_region = \"us-east1\"\n[sync]\ndatasets = [\"ds\"]\n",
        )
        .unwrap()
    }

    #[test]
    fn dry_run_and_execute() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join("bq_sync.toml");
        let api = FakeApi::default();
        let dry = MaterializeOptions {
            dry_run: true,
            ..Default::default()
        };
        assert!(
            materialize_resource(&api, &config(), &cfg, "proj/ds/view_x", &dry)
                .unwrap()
                .is_none()
        );
        assert!(api.calls().is_empty());

        let opts = MaterializeOptions {
            yes: true,
            ..Default::default()
        };
        let path = materialize_resource(&api, &config(), &cfg, "proj/ds/views/view_x.sql", &opts)
            .unwrap()
            .unwrap();
        assert!(path.ends_with("proj/ds/models/materialize_x.yaml"));
        assert!(path.is_file());
        let calls = api.calls();
        assert_eq!(calls[0], "run_query proj @Some(\"us-east1\")");
        assert_eq!(calls[1], "get_table_info proj.ds.materialize_x");

        // Naming the materialize side without a scheduled query falls back
        // to the DDL from the matching view.
        api.views.borrow_mut().push(crate::resources::ViewInfo {
            name: "view_y".into(),
            ..Default::default()
        });
        let before = api.calls().len();
        materialize_resource(&api, &config(), &cfg, "proj/ds/materialize_y", &opts).unwrap();
        assert_eq!(
            api.calls()[before..],
            [
                "run_query proj @Some(\"us-east1\")",
                "get_table_info proj.ds.materialize_y"
            ]
        );
        assert!(
            materialize_resource(&api, &config(), &cfg, "proj/ds/materialize_z", &opts).is_err()
        );

        // A scheduled query named after the target is run instead of the DDL,
        // whichever side of the pair is given.
        api.transfer_configs
            .borrow_mut()
            .push(crate::resources::TransferConfig {
                name: "projects/1/locations/us-east1/transferConfigs/9".into(),
                display_name: "materialize_x".into(),
                data_source_id: "scheduled_query".into(),
                query: "CREATE OR REPLACE TABLE `proj.ds.materialize_x`\nCLUSTER BY a AS SELECT 1"
                    .into(),
                ..Default::default()
            });
        for given in ["proj/ds/view_x", "proj/ds/models/materialize_x.yaml"] {
            let before = api.calls().len();
            materialize_resource(&api, &config(), &cfg, given, &opts).unwrap();
            assert_eq!(
                api.calls()[before..],
                [
                    "run_transfer_and_wait projects/1/locations/us-east1/transferConfigs/9",
                    "get_table_info proj.ds.materialize_x"
                ]
            );
        }

        let custom = MaterializeOptions {
            yes: true,
            target_name: Some("snap".into()),
            ..Default::default()
        };
        let path = materialize_resource(&api, &config(), &cfg, "proj/ds/view_x", &custom)
            .unwrap()
            .unwrap();
        assert!(path.ends_with("snap.yaml"));
    }
}
