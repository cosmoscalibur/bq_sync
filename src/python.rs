//! Python bindings (``bq_sync._bq_sync``), built with PyO3 / maturin.
//!
//! Long-running operations release the GIL.  Failures raise
//! ``BqSyncError`` (``NotFoundError`` for missing GCP resources, ``OSError``
//! for filesystem errors).

use std::path::PathBuf;

use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyOSError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::api::GcpClient;
use crate::cli::{self, Command, ConfigArg};
use crate::config::{self, resolve_output_dir};
use crate::error::Error;
use crate::resources::DataFormat;
use crate::sql_check::{CheckResult, CheckSummary};
use crate::{data, logging, readers, sql_check, utils};

create_exception!(
    _bq_sync,
    BqSyncError,
    PyException,
    "Base error raised by bq-sync."
);
create_exception!(
    _bq_sync,
    NotFoundError,
    BqSyncError,
    "A BigQuery resource does not exist."
);

fn to_py(e: Error) -> PyErr {
    match e {
        Error::NotFound(msg) => NotFoundError::new_err(msg),
        Error::Io { path, source } => PyOSError::new_err(format!("{}: {source}", path.display())),
        Error::Exit(code) => BqSyncError::new_err(format!(
            "bq-sync failed (exit status {code}); see log output"
        )),
        Error::Other(e) => BqSyncError::new_err(format!("{e:#}")),
        other => BqSyncError::new_err(other.to_string()),
    }
}

type PyRes<T> = PyResult<T>;

fn config_arg(config: Option<PathBuf>) -> ConfigArg {
    ConfigArg { config }
}

/// Run the ``bq-sync`` CLI with *argv* (default: ``sys.argv[1:]``) and
/// return its exit status.
#[pyfunction]
#[pyo3(signature = (argv=None))]
fn main(py: Python<'_>, argv: Option<Vec<String>>) -> PyRes<i32> {
    let argv = match argv {
        Some(a) => a,
        None => {
            let sys_argv: Vec<String> = py.import("sys")?.getattr("argv")?.extract()?;
            sys_argv.into_iter().skip(1).collect()
        }
    };
    let args: Vec<String> = std::iter::once("bq-sync".to_owned()).chain(argv).collect();
    Ok(py.detach(move || cli::run(args)))
}

/// Enable or disable DEBUG logging for library calls.
#[pyfunction]
fn set_verbose(verbose: bool) {
    logging::init(verbose);
}

/// Human-readable binary size (``"1.5 GiB"``; ``""`` for ``None``).
#[pyfunction]
#[pyo3(signature = (n))]
fn humanize_bytes(n: Option<i64>) -> String {
    utils::humanize_bytes(n)
}

/// Split ``<project>/<dataset>/[<dir>/]<name>[.ext]`` into its parts.
#[pyfunction]
fn parse_resource_path(path: &str) -> PyRes<(String, String, String)> {
    utils::parse_resource_path(path).map_err(|_| {
        BqSyncError::new_err(format!(
            "Invalid resource path '{path}': expected <project>/<dataset>/<name> or \
             <project>/<dataset>/<resource_dir>/<name>."
        ))
    })
}

/// Find ``bq_sync.toml`` from *start* (default: CWD) upward.
#[pyfunction]
#[pyo3(signature = (start=None))]
fn discover_config(start: Option<PathBuf>) -> PyRes<PathBuf> {
    config::discover_config(start.as_deref()).map_err(to_py)
}

/// Load a ``bq_sync.toml`` file into a dict.
#[pyfunction]
fn load_config<'py>(py: Python<'py>, path: PathBuf) -> PyRes<Bound<'py, PyDict>> {
    let cfg = config::load_config(&path).map_err(to_py)?;
    let project = PyDict::new(py);
    project.set_item("id", cfg.project.id)?;
    project.set_item("default_region", cfg.project.default_region)?;
    let d = PyDict::new(py);
    d.set_item("project", project)?;
    d.set_item("datasets", cfg.datasets)?;
    d.set_item("output_dir", cfg.output_dir)?;
    Ok(d)
}

/// Project-scoped output directory for the config at *config_path*.
#[pyfunction]
#[pyo3(name = "resolve_output_dir")]
fn py_resolve_output_dir(config_path: PathBuf) -> PyRes<PathBuf> {
    let path = config::absolute_resolved(&config_path);
    let cfg = config::load_config(&path).map_err(to_py)?;
    Ok(resolve_output_dir(&cfg, &path))
}

// ---------------------------------------------------------------------------
// Readers
// ---------------------------------------------------------------------------

#[pyfunction]
fn read_view_sql<'py>(py: Python<'py>, path: PathBuf) -> PyRes<Bound<'py, PyDict>> {
    let u = readers::read_view_sql(&path).map_err(to_py)?;
    let d = PyDict::new(py);
    d.set_item("name", u.name)?;
    d.set_item("sql", u.sql)?;
    Ok(d)
}

#[pyfunction]
fn read_saved_query_sql<'py>(py: Python<'py>, path: PathBuf) -> PyRes<Bound<'py, PyDict>> {
    let u = readers::read_saved_query_sql(&path).map_err(to_py)?;
    let d = PyDict::new(py);
    d.set_item("name", u.name)?;
    d.set_item("sql", u.sql)?;
    Ok(d)
}

#[pyfunction]
fn read_scheduled_query_sql<'py>(py: Python<'py>, path: PathBuf) -> PyRes<Bound<'py, PyDict>> {
    let u = readers::read_scheduled_query_sql(&path).map_err(to_py)?;
    let d = PyDict::new(py);
    d.set_item("name", u.name)?;
    d.set_item("sql", u.sql)?;
    Ok(d)
}

#[pyfunction]
fn read_routine_sql<'py>(py: Python<'py>, path: PathBuf) -> PyRes<Bound<'py, PyDict>> {
    let u = readers::read_routine_sql(&path).map_err(to_py)?;
    let d = PyDict::new(py);
    d.set_item("name", u.name)?;
    d.set_item("body", u.body)?;
    d.set_item("language", u.language)?;
    Ok(d)
}

#[pyfunction]
fn read_model_yaml<'py>(py: Python<'py>, path: PathBuf) -> PyRes<Bound<'py, PyDict>> {
    let u = readers::read_model_yaml(&path).map_err(to_py)?;
    let d = PyDict::new(py);
    d.set_item("name", u.name)?;
    d.set_item("description", u.description)?;
    d.set_item("field_descriptions", u.field_descriptions)?;
    Ok(d)
}

#[pyfunction]
fn read_routine_model_yaml<'py>(py: Python<'py>, path: PathBuf) -> PyRes<Bound<'py, PyDict>> {
    let u = readers::read_routine_model_yaml(&path).map_err(to_py)?;
    let d = PyDict::new(py);
    d.set_item("name", u.name)?;
    d.set_item("description", u.description)?;
    d.set_item("language", u.language)?;
    let args: Vec<Bound<'py, PyDict>> = u
        .arguments
        .into_iter()
        .map(|a| {
            let ad = PyDict::new(py);
            ad.set_item("name", a.name)?;
            ad.set_item("type", a.arg_type)?;
            ad.set_item("mode", a.mode)?;
            Ok(ad)
        })
        .collect::<PyRes<_>>()?;
    d.set_item("arguments", args)?;
    d.set_item("return_type", u.return_type)?;
    Ok(d)
}

/// ``(name, schedule)`` from a scheduled query SQL header.
#[pyfunction]
fn parse_schedule_from_file(path: PathBuf) -> PyRes<(String, String)> {
    crate::reschedule::parse_schedule_from_file(&path).map_err(to_py)
}

// ---------------------------------------------------------------------------
// check
// ---------------------------------------------------------------------------

fn result_dict<'py>(py: Python<'py>, r: CheckResult) -> PyRes<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("path", r.path)?;
    d.set_item("level", r.level.as_str())?;
    d.set_item("errors", r.errors)?;
    d.set_item("warnings", r.warnings)?;
    d.set_item("info", r.info)?;
    d.set_item("tables_referenced", r.tables_referenced)?;
    d.set_item("tables_resolved", r.tables_resolved)?;
    d.set_item("tables_unresolved", r.tables_unresolved)?;
    d.set_item("columns_unresolved", r.columns_unresolved)?;
    Ok(d)
}

fn summary_dict<'py>(py: Python<'py>, s: CheckSummary) -> PyRes<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("passed", s.passed)?;
    d.set_item("failed", s.failed)?;
    d.set_item("warned", s.warned)?;
    let results: Vec<_> = s
        .results
        .into_iter()
        .map(|r| result_dict(py, r))
        .collect::<PyRes<_>>()?;
    d.set_item("results", results)?;
    Ok(d)
}

/// SQL files of an output tree.
#[pyfunction]
fn discover_sql_files(output_root: PathBuf) -> Vec<PathBuf> {
    sql_check::discover_sql_files(&output_root)
}

/// Validate *files* against the catalog built from *output_root*.
#[pyfunction]
#[pyo3(signature = (files, output_root, project, online=false))]
fn check_files<'py>(
    py: Python<'py>,
    files: Vec<PathBuf>,
    output_root: PathBuf,
    project: String,
    online: bool,
) -> PyRes<Bound<'py, PyDict>> {
    let summary = py.detach(move || -> Result<CheckSummary, Error> {
        let api = if online {
            Some(GcpClient::new()?)
        } else {
            None
        };
        Ok(sql_check::check_files(
            &files,
            &output_root,
            &project,
            api.as_ref().map(|a| a as &dyn crate::api::BqApi),
        ))
    });
    summary_dict(py, summary.map_err(to_py)?)
}

/// Validate *paths* (default: every SQL file of the configured project).
#[pyfunction]
#[pyo3(signature = (paths=None, config=None, online=false))]
fn check<'py>(
    py: Python<'py>,
    paths: Option<Vec<PathBuf>>,
    config: Option<PathBuf>,
    online: bool,
) -> PyRes<Bound<'py, PyDict>> {
    let (path, cfg) = cli::resolve_config(&config_arg(config)).map_err(to_py)?;
    let output_root = resolve_output_dir(&cfg, &path);
    let files = match paths {
        Some(p) if !p.is_empty() => p.iter().map(|f| config::absolute_resolved(f)).collect(),
        _ => sql_check::discover_sql_files(&output_root),
    };
    check_files(py, files, output_root, cfg.project.id, online)
}

// ---------------------------------------------------------------------------
// GCP operations
// ---------------------------------------------------------------------------

/// Pull BigQuery resources; returns the project output directory.
#[pyfunction]
#[pyo3(signature = (config=None, dataset=None, dry_run=false, force=false, force_files=None))]
fn pull(
    py: Python<'_>,
    config: Option<PathBuf>,
    dataset: Option<String>,
    dry_run: bool,
    force: bool,
    force_files: Option<Vec<String>>,
) -> PyRes<PathBuf> {
    py.detach(move || {
        let (path, mut cfg) = cli::resolve_config(&config_arg(config))?;
        if let Some(ds) = dataset {
            cfg = cfg.with_datasets(vec![ds]);
        }
        let opts = crate::pull::PullOptions {
            dry_run,
            force,
            force_files,
        };
        crate::pull::pull_project(&GcpClient::new()?, &cfg, &path, &opts)
    })
    .map_err(to_py)
}

/// Run *command* with the GIL released.
fn execute(command: Command) -> PyRes<()> {
    Python::attach(|py| py.detach(move || cli::execute(command))).map_err(to_py)
}

/// Push files (manual mode) or detected changes (auto mode).
#[pyfunction]
#[pyo3(signature = (paths=None, data=None, since=None, dry_run=false, yes=false, include_models=false, config=None))]
fn push(
    paths: Option<Vec<String>>,
    data: Option<(String, String)>,
    since: Option<f64>,
    dry_run: bool,
    yes: bool,
    include_models: bool,
    config: Option<PathBuf>,
) -> PyRes<()> {
    let command = Command::Push {
        paths: paths.unwrap_or_default(),
        data: data.map(|(s, d)| vec![s, d]),
        since,
        dry_run,
        yes,
        include_models,
        config: config_arg(config),
    };
    execute(command)
}

/// Download data to CSV / Parquet; returns the written file.
#[pyfunction]
#[pyo3(signature = (model, format="csv", output_dir=None, config=None))]
fn fetch(
    py: Python<'_>,
    model: String,
    format: &str,
    output_dir: Option<PathBuf>,
    config: Option<PathBuf>,
) -> PyRes<PathBuf> {
    let fmt = DataFormat::parse(format).map_err(to_py)?;
    py.detach(move || {
        let loader = || {
            let (path, cfg) = cli::resolve_config(&config_arg(config.clone()))?;
            Ok((resolve_output_dir(&cfg, &path), cfg))
        };
        let data_dir = match output_dir {
            Some(dir) => config::absolute_resolved(&dir),
            None => loader()?.0.join("data"),
        };
        data::fetch_resource(&GcpClient::new()?, &model, fmt, &data_dir, &loader)
    })
    .map_err(to_py)
}

/// Delete BigQuery resources identified by local files, then the files.
#[pyfunction]
#[pyo3(signature = (paths, dry_run=false, yes=false, config=None))]
fn rm(paths: Vec<String>, dry_run: bool, yes: bool, config: Option<PathBuf>) -> PyRes<()> {
    let command = Command::Rm {
        path: paths,
        dry_run,
        yes,
        config: config_arg(config),
    };
    execute(command)
}

/// Materialize a view / external table; returns the model YAML written.
#[pyfunction]
#[pyo3(signature = (resource, target=None, dry_run=false, yes=false, config=None))]
fn materialize(
    py: Python<'_>,
    resource: String,
    target: Option<String>,
    dry_run: bool,
    yes: bool,
    config: Option<PathBuf>,
) -> PyRes<Option<PathBuf>> {
    py.detach(move || {
        let (path, cfg) = cli::resolve_config(&config_arg(config))?;
        let opts = crate::materialize::MaterializeOptions {
            dry_run,
            yes,
            target_name: target,
        };
        crate::materialize::materialize_resource(&GcpClient::new()?, &cfg, &path, &resource, &opts)
    })
    .map_err(to_py)
}

/// Change (or list / resync from files) scheduled query schedules.
#[pyfunction]
#[pyo3(signature = (display_name=None, schedule=None, from_file=false, dry_run=false, trigger=false, list=false, config=None))]
fn reschedule(
    display_name: Option<String>,
    schedule: Option<String>,
    from_file: bool,
    dry_run: bool,
    trigger: bool,
    list: bool,
    config: Option<PathBuf>,
) -> PyRes<()> {
    if from_file && schedule.is_some() {
        return Err(BqSyncError::new_err(
            "'schedule' and 'from_file' are mutually exclusive",
        ));
    }
    let command = Command::Reschedule {
        display_name,
        schedule,
        from_file,
        dry_run,
        trigger,
        list,
        config: config_arg(config),
    };
    execute(command)
}

/// Scheduled queries (``display_name``, ``schedule``) of the project.
#[pyfunction]
#[pyo3(signature = (config=None))]
fn list_scheduled_queries(py: Python<'_>, config: Option<PathBuf>) -> PyRes<Vec<(String, String)>> {
    py.detach(move || {
        let (_, cfg) = cli::resolve_config(&config_arg(config))?;
        let api = GcpClient::new()?;
        Ok(crate::api::BqApi::list_transfer_configs(
            &api,
            &cfg.project.id,
            &cfg.project.default_region,
        )?
        .into_iter()
        .map(|tc| (tc.display_name, tc.schedule))
        .collect())
    })
    .map_err(to_py)
}

#[pymodule]
fn _bq_sync(m: &Bound<'_, PyModule>) -> PyResult<()> {
    logging::init(false);
    let py = m.py();
    m.add("__version__", crate::VERSION)?;
    m.add("BqSyncError", py.get_type::<BqSyncError>())?;
    m.add("NotFoundError", py.get_type::<NotFoundError>())?;
    m.add_function(wrap_pyfunction!(main, m)?)?;
    m.add_function(wrap_pyfunction!(set_verbose, m)?)?;
    m.add_function(wrap_pyfunction!(humanize_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(parse_resource_path, m)?)?;
    m.add_function(wrap_pyfunction!(discover_config, m)?)?;
    m.add_function(wrap_pyfunction!(load_config, m)?)?;
    m.add_function(wrap_pyfunction!(py_resolve_output_dir, m)?)?;
    m.add_function(wrap_pyfunction!(read_view_sql, m)?)?;
    m.add_function(wrap_pyfunction!(read_saved_query_sql, m)?)?;
    m.add_function(wrap_pyfunction!(read_scheduled_query_sql, m)?)?;
    m.add_function(wrap_pyfunction!(read_routine_sql, m)?)?;
    m.add_function(wrap_pyfunction!(read_model_yaml, m)?)?;
    m.add_function(wrap_pyfunction!(read_routine_model_yaml, m)?)?;
    m.add_function(wrap_pyfunction!(parse_schedule_from_file, m)?)?;
    m.add_function(wrap_pyfunction!(discover_sql_files, m)?)?;
    m.add_function(wrap_pyfunction!(check_files, m)?)?;
    m.add_function(wrap_pyfunction!(check, m)?)?;
    m.add_function(wrap_pyfunction!(pull, m)?)?;
    m.add_function(wrap_pyfunction!(push, m)?)?;
    m.add_function(wrap_pyfunction!(fetch, m)?)?;
    m.add_function(wrap_pyfunction!(rm, m)?)?;
    m.add_function(wrap_pyfunction!(materialize, m)?)?;
    m.add_function(wrap_pyfunction!(reschedule, m)?)?;
    m.add_function(wrap_pyfunction!(list_scheduled_queries, m)?)?;
    Ok(())
}
