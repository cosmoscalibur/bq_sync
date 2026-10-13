//! Command-line interface (``bq-sync``).

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use clap::{Args, Parser, Subcommand, ValueEnum};

use crate::api::GcpClient;
use crate::config::{
    SyncConfig, absolute_resolved, discover_config, load_config, resolve_output_dir,
};
use crate::error::{Error, Result};
use crate::resources::DataFormat;
use crate::sql_check::{self, Level};
use crate::{data, logging, materialize, pull, push, reschedule};

#[derive(Debug, Parser)]
#[command(
    name = "bq-sync",
    version,
    about = "Sync BigQuery resources to a local directory."
)]
pub struct Cli {
    /// Enable verbose (DEBUG) logging.
    #[arg(short, long, global = true)]
    pub verbose: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Args, Clone, Default)]
pub struct ConfigArg {
    /// Path to bq_sync.toml (default: auto-discover from CWD).
    #[arg(long)]
    pub config: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum FormatArg {
    Csv,
    Parquet,
}

impl From<FormatArg> for DataFormat {
    fn from(f: FormatArg) -> Self {
        match f {
            FormatArg::Csv => DataFormat::Csv,
            FormatArg::Parquet => DataFormat::Parquet,
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Fetch BQ resources to local files.
    Pull {
        #[command(flatten)]
        config: ConfigArg,
        /// Sync a single dataset (default: all configured).
        #[arg(long)]
        dataset: Option<String>,
        /// Preview actions without writing files.
        #[arg(long)]
        dry_run: bool,
        /// Force fetch all files, bypassing decision matrix.
        #[arg(long)]
        force: bool,
        /// Force fetch a specific file (repeatable).
        #[arg(long = "force-file", value_name = "FORCE_FILE")]
        force_file: Vec<String>,
    },
    /// Deploy local resources to BigQuery.
    Push {
        /// Manual mode: files to push.
        paths: Vec<String>,
        /// Table-replace: local CSV/Parquet path and project/dataset/table
        /// destination (manual only).
        #[arg(long, num_args = 2, value_names = ["SOURCE", "DEST"])]
        data: Option<Vec<String>>,
        /// Auto mode: use mtime-based detection with the given look-back
        /// window in hours (skips git detection).
        #[arg(long)]
        since: Option<f64>,
        /// Preview changeset without executing writes.
        #[arg(long)]
        dry_run: bool,
        /// Skip interactive confirmation prompt.
        #[arg(short, long)]
        yes: bool,
        /// Auto mode: include materialized model YAMLs (excluded by default).
        #[arg(long)]
        include_models: bool,
        #[command(flatten)]
        config: ConfigArg,
    },
    /// Download table/view data as CSV or Parquet.
    Fetch {
        /// BigQuery resource path: <project>/<dataset>/<model>.
        model: String,
        /// Output format.
        #[arg(short, long, value_enum, default_value = "csv")]
        format: FormatArg,
        /// Directory for output files (used as-is; default: config data dir).
        #[arg(short, long)]
        output_dir: Option<PathBuf>,
        #[command(flatten)]
        config: ConfigArg,
    },
    /// Delete BQ resources and their local files.
    Rm {
        /// Local file paths identifying BQ resources to delete.
        #[arg(required = true)]
        path: Vec<String>,
        /// Preview without deleting.
        #[arg(long)]
        dry_run: bool,
        /// Skip interactive confirmation.
        #[arg(short, long)]
        yes: bool,
        #[command(flatten)]
        config: ConfigArg,
    },
    /// Validate SQL syntax, references, and column lineage.
    Check {
        /// SQL files to validate (default: auto-discover all).
        paths: Vec<PathBuf>,
        #[command(flatten)]
        config: ConfigArg,
        /// Enable BQ dry-run validation for JS routines. Requires active GCP
        /// credentials.
        #[arg(long)]
        online: bool,
    },
    /// Create a permanent table from a view or external table.
    Materialize {
        /// Resource path: <project>/<dataset>/<name>.
        resource: String,
        /// Target table name (default: replaces view_/external_ prefix with
        /// materialize_).
        #[arg(long)]
        target: Option<String>,
        /// Print DDL without executing.
        #[arg(long)]
        dry_run: bool,
        /// Skip interactive confirmation.
        #[arg(short, long)]
        yes: bool,
        #[command(flatten)]
        config: ConfigArg,
    },
    /// Change the schedule of a BigQuery scheduled query.
    Reschedule {
        /// Exact display_name of the scheduled query.
        display_name: Option<String>,
        /// New schedule string (e.g. "every 24 hours").
        #[arg(long, conflicts_with = "from_file")]
        schedule: Option<String>,
        /// Read schedule from SQL file headers. With display_name: sync that
        /// query's file. Without: batch-sync all files in scheduled_queries/.
        #[arg(long)]
        from_file: bool,
        /// Preview the change without applying it.
        #[arg(long)]
        dry_run: bool,
        /// Trigger a manual run after updating the schedule.
        #[arg(long)]
        trigger: bool,
        /// List all scheduled queries and exit.
        #[arg(long)]
        list: bool,
        #[command(flatten)]
        config: ConfigArg,
    },
}

/// Parse *args* (including the program name), run, and return the exit code.
pub fn run<I, T>(args: I) -> i32
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let cli = match Cli::try_parse_from(args) {
        Ok(cli) => cli,
        Err(e) => {
            let _ = e.print();
            return e.exit_code();
        }
    };
    logging::init(cli.verbose);
    match execute(cli.command) {
        Ok(()) => 0,
        Err(Error::Exit(code)) => code,
        Err(Error::Other(e)) => {
            log::error!("{e:#}");
            1
        }
        Err(e) => {
            log::error!("{e}");
            1
        }
    }
}

/// Discover / load the configuration.
pub fn resolve_config(arg: &ConfigArg) -> Result<(PathBuf, SyncConfig)> {
    let path = match &arg.config {
        Some(p) => absolute_resolved(p),
        None => discover_config(None)
            .inspect_err(|e| log::error!("{e}"))
            .map_err(|_| Error::Exit(1))?,
    };
    let config = load_config(&path)?;
    Ok((path, config))
}

fn client() -> Result<GcpClient> {
    GcpClient::new()
}

/// Run a parsed subcommand.
pub fn execute(command: Command) -> Result<()> {
    match command {
        Command::Pull {
            config,
            dataset,
            dry_run,
            force,
            force_file,
        } => {
            let (path, mut cfg) = resolve_config(&config)?;
            if let Some(ds) = dataset {
                cfg = cfg.with_datasets(vec![ds]);
            }
            let opts = pull::PullOptions {
                dry_run,
                force,
                force_files: (!force_file.is_empty()).then_some(force_file),
            };
            pull::pull_project(&client()?, &cfg, &path, &opts)?;
        }
        Command::Push {
            paths,
            data,
            since,
            dry_run,
            yes,
            include_models,
            config,
        } => {
            let (path, cfg) = resolve_config(&config)?;
            let data_spec = match data.as_deref() {
                Some([source, dest]) => Some(push::DataSpec::parse(source, dest)?),
                _ => None,
            };
            let opts = push::PushOptions {
                dry_run,
                yes,
                since_hours: since.unwrap_or(24.0),
                include_models,
                use_mtime: since.is_some(),
            };
            let api = client()?;
            if !paths.is_empty() || data_spec.is_some() {
                push::push_manual(&api, &cfg, &path, &paths, data_spec.as_ref(), &opts)?;
            } else {
                push::push_auto(&api, &cfg, &path, &opts)?;
            }
        }
        Command::Fetch {
            model,
            format,
            output_dir,
            config,
        } => {
            let loader = || {
                let (path, cfg) = resolve_config(&config)?;
                Ok((resolve_output_dir(&cfg, &path), cfg))
            };
            let data_dir = match output_dir {
                Some(dir) => absolute_resolved(&dir),
                None => loader()?.0.join("data"),
            };
            data::fetch_resource(&client()?, &model, format.into(), &data_dir, &loader)?;
        }
        Command::Rm {
            path: paths,
            dry_run,
            yes,
            config,
        } => {
            let (path, cfg) = resolve_config(&config)?;
            let opts = push::PushOptions {
                dry_run,
                yes,
                ..Default::default()
            };
            push::rm_resources(&client()?, &cfg, &path, &paths, &opts)?;
        }
        Command::Check {
            paths,
            config,
            online,
        } => check(&paths, &config, online)?,
        Command::Materialize {
            resource,
            target,
            dry_run,
            yes,
            config,
        } => {
            let (path, cfg) = resolve_config(&config)?;
            let opts = materialize::MaterializeOptions {
                dry_run,
                yes,
                target_name: target,
            };
            materialize::materialize_resource(&client()?, &cfg, &path, &resource, &opts)?;
        }
        Command::Reschedule {
            display_name,
            schedule,
            from_file,
            dry_run,
            trigger,
            list,
            config,
        } => {
            let (path, cfg) = resolve_config(&config)?;
            let api = client()?;
            if list {
                return reschedule::list_scheduled_queries(&api, &cfg);
            }
            let opts = reschedule::RescheduleOptions { dry_run, trigger };
            if from_file {
                let sq_dir = resolve_output_dir(&cfg, &path).join("scheduled_queries");
                return match display_name {
                    Some(name) => {
                        let file = sq_dir.join(format!("{name}.sql"));
                        if !file.is_file() {
                            log::error!("File not found: {}", file.display());
                            return Err(Error::Exit(1));
                        }
                        reschedule::resync_from_file(&api, &cfg, &file, opts)
                    }
                    None => {
                        if !sq_dir.is_dir() {
                            log::error!(
                                "Scheduled queries directory not found: {}",
                                sq_dir.display()
                            );
                            return Err(Error::Exit(1));
                        }
                        reschedule::resync_all_from_files(&api, &cfg, &sq_dir, dry_run)
                    }
                };
            }
            let (Some(name), Some(schedule)) = (display_name, schedule) else {
                log::error!(
                    "reschedule requires <display_name> and --schedule (or use --list / --from-file)."
                );
                return Err(Error::Exit(1));
            };
            reschedule::reschedule_query(&api, &cfg, &name, &schedule, opts)?;
        }
    }
    Ok(())
}

fn check(paths: &[PathBuf], config: &ConfigArg, online: bool) -> Result<()> {
    let (path, cfg) = resolve_config(config)?;
    let output_root = resolve_output_dir(&cfg, &path);
    let files: Vec<PathBuf> = if paths.is_empty() {
        sql_check::discover_sql_files(&output_root)
    } else {
        paths.iter().map(|p| absolute_resolved(p)).collect()
    };
    if files.is_empty() {
        log::info!("No SQL files to validate.");
        return Ok(());
    }
    log::info!("Validating {} SQL file(s)...\n", files.len());

    let api = if online { Some(client()?) } else { None };
    let summary = sql_check::check_files(
        &files,
        &output_root,
        &cfg.project.id,
        api.as_ref().map(|a| a as &dyn crate::api::BqApi),
    );
    print_summary(&summary, &output_root);
    if summary.failed > 0 {
        return Err(Error::Exit(1));
    }
    Ok(())
}

/// Pretty-print check results to stdout.
pub fn print_summary(summary: &sql_check::CheckSummary, output_root: &Path) {
    for result in &summary.results {
        let rel = result
            .path
            .strip_prefix(output_root)
            .map(|p| p.display().to_string())
            .unwrap_or_else(|_| {
                result
                    .path
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default()
            });
        let icon = if result.level == Level::Error {
            "✗"
        } else {
            "✓"
        };
        println!("  {icon} {rel}");
        for table in &result.tables_resolved {
            println!("      Tables: {table} ✓");
        }
        for err in &result.errors {
            println!("      {err}");
        }
        for warn in &result.warnings {
            println!("      ⚠ {warn}");
        }
        for info in &result.info {
            println!("      ⓘ {info}");
        }
    }
    println!(
        "\n  {} error(s), {} warning(s), {} passed",
        summary.failed, summary.warned, summary.passed
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> std::result::Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("bq-sync").chain(args.iter().copied()))
    }

    #[test]
    fn definition_is_valid() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
    }

    #[test]
    fn push_args() {
        let Command::Push {
            paths,
            data,
            since,
            yes,
            dry_run,
            include_models,
            ..
        } = parse(&["push"]).unwrap().command
        else {
            panic!()
        };
        assert!(
            paths.is_empty()
                && data.is_none()
                && since.is_none()
                && !yes
                && !dry_run
                && !include_models
        );

        let Command::Push {
            paths,
            data,
            since,
            yes,
            dry_run,
            include_models,
            ..
        } = parse(&[
            "push",
            "a.sql",
            "b.yaml",
            "--data",
            "x.csv",
            "p/d/t",
            "--since",
            "2",
            "-y",
            "--dry-run",
            "--include-models",
        ])
        .unwrap()
        .command
        else {
            panic!()
        };
        assert_eq!(paths, vec!["a.sql", "b.yaml"]);
        assert_eq!(data.unwrap(), vec!["x.csv", "p/d/t"]);
        assert_eq!(since, Some(2.0));
        assert!(yes && dry_run && include_models);
    }

    #[test]
    fn fetch_args() {
        let Command::Fetch {
            model,
            format,
            output_dir,
            ..
        } = parse(&["fetch", "proj/ds/tbl"]).unwrap().command
        else {
            panic!()
        };
        assert_eq!(model, "proj/ds/tbl");
        assert!(matches!(format, FormatArg::Csv));
        assert!(output_dir.is_none());
        assert!(matches!(
            parse(&["fetch", "p/d/t", "-f", "parquet", "-o", "/tmp/out"])
                .unwrap()
                .command,
            Command::Fetch {
                format: FormatArg::Parquet,
                output_dir: Some(_),
                ..
            }
        ));
        assert!(parse(&["fetch", "p/d/t", "--format", "json"]).is_err());
        assert!(parse(&["fetch"]).is_err());
    }

    #[test]
    fn rm_materialize_reschedule_args() {
        assert!(parse(&["rm"]).is_err());
        assert!(matches!(
            parse(&["rm", "a", "b", "--dry-run", "-y"]).unwrap().command,
            Command::Rm { ref path, dry_run: true, yes: true, .. } if path.len() == 2
        ));
        assert!(matches!(
            parse(&["materialize", "p/d/v", "--target", "t", "--dry-run", "-y"])
                .unwrap()
                .command,
            Command::Materialize {
                target: Some(_),
                dry_run: true,
                yes: true,
                ..
            }
        ));
        assert!(matches!(
            parse(&["reschedule", "--list"]).unwrap().command,
            Command::Reschedule {
                list: true,
                from_file: false,
                ..
            }
        ));
        assert!(matches!(
            parse(&[
                "reschedule",
                "q",
                "--schedule",
                "every 24 hours",
                "--trigger"
            ])
            .unwrap()
            .command,
            Command::Reschedule {
                display_name: Some(_),
                schedule: Some(_),
                trigger: true,
                ..
            }
        ));
        assert!(parse(&["reschedule", "--from-file", "--schedule", "x"]).is_err());
    }

    #[test]
    fn version_flag() {
        let err = parse(&["--version"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::DisplayVersion);
        assert_eq!(err.exit_code(), 0);
    }

    #[test]
    fn check_end_to_end_exit_codes() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join("bq_sync.toml");
        std::fs::write(
            &cfg,
            "[project]\nid = \"p\"\ndefault_region = \"r\"\n[sync]\ndatasets = [\"d\"]\n",
        )
        .unwrap();
        let views = dir.path().join("p/d/views");
        std::fs::create_dir_all(&views).unwrap();
        std::fs::write(views.join("ok.sql"), "SELECT 1").unwrap();
        let cfg_s = cfg.to_string_lossy().into_owned();
        assert_eq!(run(["bq-sync", "check", "--config", &cfg_s]), 0);
        std::fs::write(views.join("bad.sql"), "SELECT FROM WHERE").unwrap();
        assert_eq!(run(["bq-sync", "check", "--config", &cfg_s]), 1);
    }
}
