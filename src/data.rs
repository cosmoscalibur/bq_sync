//! ``fetch`` — download table / view / saved query data as CSV or Parquet.

use std::path::{Path, PathBuf};

use crate::api::BqApi;
use crate::config::SyncConfig;
use crate::error::{Error, Result};
use crate::resources::DataFormat;
use crate::utils::stem;

/// Lazily resolved configuration: ``(output_root, config)``.
pub type ConfigLoader<'a> = dyn Fn() -> Result<(PathBuf, SyncConfig)> + 'a;

/// Download the data identified by *model* into ``data_dir/<name>.<fmt>``.
///
/// Accepted paths:
///
/// - ``<project>/<dataset>/<table_or_view>`` — rows via the BigQuery API.
/// - ``<project>/<dataset>/<resource_dir>/<name>[.sql]`` — with ``.sql``,
///   the local SQL file (when present) is executed instead.
/// - ``<project>/saved_queries/<name>[.sql]`` — the local SQL file when
///   present (``.sql``), otherwise the saved query from Dataform.
///
/// Returns the written file path.
pub fn fetch_resource(
    api: &dyn BqApi,
    model: &str,
    fmt: DataFormat,
    data_dir: &Path,
    load_config: &ConfigLoader<'_>,
) -> Result<PathBuf> {
    let parts: Vec<&str> = model.split('/').collect();
    let ends_with_sql = parts.last().is_some_and(|p| p.ends_with(".sql"));
    let ext = fmt.extension();

    if let [project, "saved_queries", file] = parts.as_slice() {
        let name = stem(file);
        let dest = data_dir.join(format!("{name}.{ext}"));
        let (output_root, config) = load_config()?;
        if ends_with_sql {
            let sql_path = output_root
                .join("saved_queries")
                .join(format!("{name}.sql"));
            if sql_path.is_file() {
                return run_local_sql(api, project, &sql_path, &dest, fmt);
            }
        }
        let region = &config.project.default_region;
        let Some(saved) = api
            .list_saved_queries(project, region)?
            .into_iter()
            .find(|s| s.name == name)
        else {
            log::error!("Saved query '{name}' not found in project '{project}' region '{region}'.");
            return Err(Error::Exit(1));
        };
        log::info!("Executing saved query '{name}' -> {}", dest.display());
        api.fetch_query_to_file(project, &saved.sql, &dest, fmt)?;
        log::info!("Saved {}", dest.display());
        return Ok(dest);
    }

    let (project, dataset, resource_type, name) = match parts.as_slice() {
        [p, d, r, n] => (*p, *d, Some(*r), *n),
        [p, d, n] => (*p, *d, None, *n),
        _ => {
            log::error!(
                "Invalid model path '{model}': expected \
                 <project>/<dataset>/<table_or_view>, \
                 <project>/<dataset>/<resource_type>/<table_or_view>, or \
                 <project>/saved_queries/<name> but got {} segments. \
                 Example: my-project/my_dataset/my_view",
                parts.len()
            );
            return Err(Error::Exit(1));
        }
    };
    let table = stem(name);
    let dest = data_dir.join(format!("{table}.{ext}"));

    if ends_with_sql && let Some(rtype) = resource_type {
        let (output_root, _) = load_config()?;
        let sql_path = output_root
            .join(dataset)
            .join(rtype)
            .join(format!("{table}.sql"));
        if sql_path.is_file() {
            return run_local_sql(api, project, &sql_path, &dest, fmt);
        }
    }

    log::info!("Fetching {model} -> {}", dest.display());
    api.fetch_table_to_file(project, dataset, &table, &dest, fmt)?;
    log::info!("Saved {}", dest.display());
    Ok(dest)
}

fn run_local_sql(
    api: &dyn BqApi,
    project: &str,
    sql_path: &Path,
    dest: &Path,
    fmt: DataFormat,
) -> Result<PathBuf> {
    let sql = std::fs::read_to_string(sql_path).map_err(|e| Error::io(sql_path, e))?;
    log::info!(
        "Executing local SQL {} -> {}",
        sql_path.display(),
        dest.display()
    );
    api.fetch_query_to_file(project, &sql, dest, fmt)?;
    log::info!("Saved {}", dest.display());
    Ok(dest.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::{EPOCH, SavedQueryInfo};
    use crate::testing::FakeApi;

    fn setup() -> (tempfile::TempDir, PathBuf, SyncConfig) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("proj");
        let cfg = SyncConfig::from_toml(
            "[project]\nid = \"proj\"\ndefault_region = \"us-east1\"\n[sync]\ndatasets = [\"ds\"]\n",
        )
        .unwrap();
        (dir, root, cfg)
    }

    #[test]
    fn table_paths() {
        let (_d, root, cfg) = setup();
        let api = FakeApi::default();
        let loader = || Ok((root.clone(), cfg.clone()));
        let data = root.join("data");

        let out = fetch_resource(&api, "proj/ds/tbl", DataFormat::Csv, &data, &loader).unwrap();
        assert_eq!(out, data.join("tbl.csv"));
        fetch_resource(
            &api,
            "proj/ds/models/tbl.yaml",
            DataFormat::Parquet,
            &data,
            &loader,
        )
        .unwrap();
        // `.sql` without a local file falls back to the table API.
        fetch_resource(&api, "proj/ds/views/v.sql", DataFormat::Csv, &data, &loader).unwrap();
        let calls = api.calls();
        assert!(calls[0].starts_with("fetch_table_to_file proj.ds.tbl"));
        assert!(
            calls[1].starts_with("fetch_table_to_file proj.ds.tbl")
                && calls[1].ends_with("Parquet")
        );
        assert!(calls[2].starts_with("fetch_table_to_file proj.ds.v"));

        assert!(matches!(
            fetch_resource(&api, "a/b", DataFormat::Csv, &data, &loader),
            Err(Error::Exit(1))
        ));
    }

    #[test]
    fn local_sql_and_saved_queries() {
        let (_d, root, cfg) = setup();
        let views = root.join("ds/views");
        std::fs::create_dir_all(&views).unwrap();
        std::fs::write(views.join("v.sql"), "SELECT 1").unwrap();
        let sq = root.join("saved_queries");
        std::fs::create_dir_all(&sq).unwrap();
        std::fs::write(sq.join("local.sql"), "SELECT 2").unwrap();

        let api = FakeApi::default();
        *api.saved.borrow_mut() = vec![SavedQueryInfo {
            name: "remote".into(),
            sql: "SELECT 3".into(),
            modified: EPOCH,
        }];
        let loader = || Ok((root.clone(), cfg.clone()));
        let data = root.join("data");

        fetch_resource(&api, "proj/ds/views/v.sql", DataFormat::Csv, &data, &loader).unwrap();
        fetch_resource(
            &api,
            "proj/saved_queries/local.sql",
            DataFormat::Csv,
            &data,
            &loader,
        )
        .unwrap();
        fetch_resource(
            &api,
            "proj/saved_queries/remote",
            DataFormat::Csv,
            &data,
            &loader,
        )
        .unwrap();
        let calls = api.calls();
        assert!(calls[0].starts_with("fetch_query_to_file proj SELECT 1"));
        assert!(calls[1].starts_with("fetch_query_to_file proj SELECT 2"));
        assert!(calls[2].starts_with("fetch_query_to_file proj SELECT 3"));
        assert!(matches!(
            fetch_resource(
                &api,
                "proj/saved_queries/missing",
                DataFormat::Csv,
                &data,
                &loader
            ),
            Err(Error::Exit(1))
        ));
    }
}
