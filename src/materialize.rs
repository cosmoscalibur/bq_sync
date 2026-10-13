//! Materialize a view or external table into a permanent table.
//!
//! Runs ``CREATE OR REPLACE TABLE <target> AS SELECT * FROM <source>``,
//! then writes the new table's model YAML so the local tree stays in sync.

use std::path::{Path, PathBuf};

use crate::api::BqApi;
use crate::config::{SyncConfig, resolve_output_dir};
use crate::error::Result;
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

/// DDL that (re)creates *target_name* from *source_name*.
pub fn build_ddl(project: &str, dataset: &str, source_name: &str, target_name: &str) -> String {
    format!(
        "CREATE OR REPLACE TABLE `{project}.{dataset}.{target_name}`\n\
         AS SELECT * FROM `{project}.{dataset}.{source_name}`"
    )
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
    let (project, dataset, source) = parse_resource_path(resource_path)?;
    let target = opts
        .target_name
        .clone()
        .filter(|t| !t.is_empty())
        .unwrap_or_else(|| default_target_name(&source));
    let ddl = build_ddl(&project, &dataset, &source, &target);

    println!("\n  Source : {project}.{dataset}.{source}");
    println!("  Target : {project}.{dataset}.{target}");
    println!("\n  DDL:\n\n    {}\n", ddl.replace('\n', "\n    "));

    if opts.dry_run {
        log::info!("Dry-run: no changes written.");
        return Ok(None);
    }
    if !opts.yes && !confirm("Proceed with materialization?") {
        log::info!("Materialization cancelled.");
        return Ok(None);
    }

    log::info!("Materializing {project}.{dataset}.{source} -> {target} …");
    api.run_query(&project, &ddl, Some(location))?;
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
