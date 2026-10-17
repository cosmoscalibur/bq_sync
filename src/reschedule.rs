//! Reschedule orchestrator — change the schedule of scheduled queries.

use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use regex::Regex;

use crate::api::BqApi;
use crate::config::SyncConfig;
use crate::error::{Error, Result};

static RE_NAME: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^--\s*Scheduled Query:\s*(.+)$").unwrap());
static RE_SCHEDULE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^--\s*Schedule:\s*(.+)$").unwrap());

fn or_none(s: &str) -> &str {
    if s.is_empty() { "(none)" } else { s }
}

/// Print every scheduled query of the configured project / region.
pub fn list_scheduled_queries(api: &dyn BqApi, config: &SyncConfig) -> Result<()> {
    let project = &config.project.id;
    let region = &config.project.default_region;
    let configs = api.list_transfer_configs(project, region)?;
    if configs.is_empty() {
        log::info!("No scheduled queries found in {project}/{region}.");
        return Ok(());
    }
    println!("\n  Scheduled queries in {project} ({region}):\n");
    for tc in configs {
        println!("    • {}", tc.display_name);
        println!("      schedule: {}", or_none(&tc.schedule));
    }
    println!();
    Ok(())
}

/// Options for [`reschedule_query`] / [`resync_from_file`].
#[derive(Debug, Clone, Copy, Default)]
pub struct RescheduleOptions {
    pub dry_run: bool,
    /// Trigger a manual run after updating.
    pub trigger: bool,
}

fn apply(
    api: &dyn BqApi,
    project: &str,
    region: &str,
    name: &str,
    schedule: &str,
    opts: RescheduleOptions,
) -> Result<()> {
    if opts.dry_run {
        println!();
        log::info!("Dry-run: no changes applied.");
        return Ok(());
    }
    api.update_transfer_schedule(project, region, name, schedule)?;
    log::info!("Schedule updated for '{name}'.");
    if opts.trigger {
        log::info!("Triggering manual run for '{name}' …");
        api.trigger_transfer_run(project, region, name)?;
        log::info!("Manual run triggered.");
    }
    Ok(())
}

/// Update the schedule of the scheduled query *display_name*.
pub fn reschedule_query(
    api: &dyn BqApi,
    config: &SyncConfig,
    display_name: &str,
    schedule: &str,
    opts: RescheduleOptions,
) -> Result<()> {
    let project = &config.project.id;
    let region = &config.project.default_region;
    let Some(current) = api.get_transfer_config(project, region, display_name)? else {
        log::error!("Scheduled query '{display_name}' not found in {project}/{region}.");
        return Err(Error::Exit(1));
    };
    println!("\n  Query   : {display_name}");
    println!("  Current : {}", or_none(&current.schedule));
    println!("  New     : {schedule}");
    apply(api, project, region, display_name, schedule, opts)
}

/// Extract ``(name, schedule)`` from a scheduled query SQL header.
pub fn parse_schedule_from_file(path: &Path) -> Result<(String, String)> {
    let text = std::fs::read_to_string(path).map_err(|e| Error::io(path, e))?;
    let mut name = None;
    let mut schedule = None;
    for line in text.lines().take(10) {
        if name.is_none() {
            name = RE_NAME.captures(line).map(|m| m[1].trim().to_owned());
        }
        if schedule.is_none() {
            schedule = RE_SCHEDULE.captures(line).map(|m| m[1].trim().to_owned());
        }
        if name.is_some() && schedule.is_some() {
            break;
        }
    }
    let name = name.ok_or_else(|| {
        Error::Invalid(format!(
            "Missing '-- Scheduled Query: <name>' header in {}",
            path.display()
        ))
    })?;
    let schedule = schedule.ok_or_else(|| {
        Error::Invalid(format!(
            "Missing '-- Schedule: <schedule>' header in {}",
            path.display()
        ))
    })?;
    Ok((name, schedule))
}

/// Sync one scheduled query's schedule from its SQL file header.
pub fn resync_from_file(
    api: &dyn BqApi,
    config: &SyncConfig,
    path: &Path,
    opts: RescheduleOptions,
) -> Result<()> {
    let (name, file_schedule) = parse_schedule_from_file(path)?;
    let project = &config.project.id;
    let region = &config.project.default_region;
    let Some(current) = api.get_transfer_config(project, region, &name)? else {
        log::error!(
            "Scheduled query '{name}' (from {}) not found in {project}/{region}.",
            path.display()
        );
        return Err(Error::Exit(1));
    };
    if current.schedule == file_schedule {
        println!("  ✓ {name}: already in sync ({file_schedule})");
        return Ok(());
    }
    println!("\n  Query   : {name}");
    println!("  Current : {}", or_none(&current.schedule));
    println!("  File    : {file_schedule}");
    apply(api, project, region, &name, &file_schedule, opts)
}

/// Sync every ``*.sql`` file of *dir* (sorted by name).
pub fn resync_all_from_files(
    api: &dyn BqApi,
    config: &SyncConfig,
    dir: &Path,
    dry_run: bool,
) -> Result<()> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .map_err(|e| Error::io(dir, e))?
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.is_file() && p.extension().is_some_and(|e| e == "sql"))
        .collect();
    files.sort();
    if files.is_empty() {
        log::info!("No .sql files found in {}.", dir.display());
        return Ok(());
    }
    println!(
        "\n  Syncing {} scheduled query file(s) from {}\n",
        files.len(),
        dir.display()
    );
    let opts = RescheduleOptions {
        dry_run,
        trigger: false,
    };
    for f in &files {
        resync_from_file(api, config, f, opts)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::TransferConfig;
    use crate::testing::FakeApi;

    fn config() -> SyncConfig {
        SyncConfig::from_toml(
            "[project]\nid = \"p\"\ndefault_region = \"us-east1\"\n[sync]\ndatasets = []\n",
        )
        .unwrap()
    }

    fn fake() -> FakeApi {
        let api = FakeApi::default();
        *api.transfer_configs.borrow_mut() = vec![TransferConfig {
            name: "projects/p/locations/us-east1/transferConfigs/1".into(),
            display_name: "daily".into(),
            schedule: "every 24 hours".into(),
            data_source_id: "scheduled_query".into(),
            ..Default::default()
        }];
        api
    }

    #[test]
    fn reschedule_flows() {
        let api = fake();
        let dry = RescheduleOptions {
            dry_run: true,
            trigger: true,
        };
        reschedule_query(&api, &config(), "daily", "every 1 hours", dry).unwrap();
        assert!(api.calls().is_empty());

        let run = RescheduleOptions {
            dry_run: false,
            trigger: true,
        };
        reschedule_query(&api, &config(), "daily", "every 1 hours", run).unwrap();
        assert_eq!(
            api.calls(),
            vec![
                "update_schedule projects/p/locations/us-east1/transferConfigs/1 every 1 hours",
                "start_manual_run projects/p/locations/us-east1/transferConfigs/1",
            ]
        );
        assert!(matches!(
            reschedule_query(&api, &config(), "missing", "x", run),
            Err(Error::Exit(1))
        ));
    }

    #[test]
    fn parse_header() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("daily.sql");
        std::fs::write(
            &p,
            "-- Scheduled Query: daily\n-- Schedule: every 2 hours\n\nSELECT 1",
        )
        .unwrap();
        assert_eq!(
            parse_schedule_from_file(&p).unwrap(),
            ("daily".to_owned(), "every 2 hours".to_owned())
        );
        std::fs::write(&p, "-- Scheduled Query: daily\nSELECT 1").unwrap();
        assert!(
            matches!(parse_schedule_from_file(&p), Err(Error::Invalid(m)) if m.contains("Schedule"))
        );
        std::fs::write(&p, "-- Schedule: x\nSELECT 1").unwrap();
        assert!(
            matches!(parse_schedule_from_file(&p), Err(Error::Invalid(m)) if m.contains("Scheduled Query"))
        );
    }

    #[test]
    fn resync_files() {
        let dir = tempfile::tempdir().unwrap();
        let api = fake();
        let p = dir.path().join("daily.sql");
        std::fs::write(
            &p,
            "-- Scheduled Query: daily\n-- Schedule: every 24 hours\n\nSELECT 1",
        )
        .unwrap();
        resync_from_file(&api, &config(), &p, RescheduleOptions::default()).unwrap();
        assert!(api.calls().is_empty(), "in sync: no update");

        std::fs::write(
            &p,
            "-- Scheduled Query: daily\n-- Schedule: every 2 hours\n\nSELECT 1",
        )
        .unwrap();
        std::fs::write(dir.path().join("notes.txt"), "ignored").unwrap();
        resync_all_from_files(&api, &config(), dir.path(), false).unwrap();
        assert_eq!(api.calls().len(), 1);

        std::fs::write(&p, "-- Scheduled Query: gone\n-- Schedule: x\n").unwrap();
        assert!(matches!(
            resync_from_file(&api, &config(), &p, RescheduleOptions::default()),
            Err(Error::Exit(1))
        ));
    }
}
