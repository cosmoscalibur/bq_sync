//! Push orchestrator — deploy local resources to BigQuery — and ``rm``.
//!
//! - **Manual**: push explicitly listed files, optionally replacing a
//!   table's data (``--data``).
//! - **Auto**: detect changed files via ``git`` (or mtime) and push them
//!   after confirmation.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime};

use crate::api::BqApi;
use crate::config::{SyncConfig, absolute_resolved, resolve_output_dir};
use crate::error::{Error, Result};
use crate::readers;
use crate::resources::{DataFormat, RoutineDefinition};
use crate::utils::{confirm, stem};

const VIEW_DIR: &str = "views";
const MODEL_DIR: &str = "models";
const ROUTINE_DIR: &str = "routines";
const SAVED_QUERY_DIR: &str = "saved_queries";
const SCHEDULED_QUERY_DIR: &str = "scheduled_queries";
const KNOWN_DIRS: [&str; 5] = [
    VIEW_DIR,
    MODEL_DIR,
    ROUTINE_DIR,
    SAVED_QUERY_DIR,
    SCHEDULED_QUERY_DIR,
];
/// Project-level resource directories (``<output_root>/<dir>/<name>``).
const PROJECT_DIRS: [&str; 2] = [SAVED_QUERY_DIR, SCHEDULED_QUERY_DIR];

/// Table-replace specification (``push --data SOURCE DEST``).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataSpec {
    pub source: PathBuf,
    pub project: String,
    pub dataset: String,
    pub table: String,
    pub fmt: DataFormat,
}

impl DataSpec {
    /// Build from ``SOURCE`` and a ``project/dataset/table`` destination.
    /// The format is inferred from the source extension.
    pub fn parse(source: &str, dest: &str) -> Result<Self> {
        let parts: Vec<&str> = dest.split('/').collect();
        let [project, dataset, table] = parts.as_slice() else {
            log::error!(
                "Invalid --data destination '{dest}': expected project/dataset/table \
                 (e.g. 'my-project/my_dataset/my_table')."
            );
            return Err(Error::Exit(1));
        };
        let source = absolute_resolved(Path::new(source));
        let fmt = if source.extension().is_some_and(|e| e == "parquet") {
            DataFormat::Parquet
        } else {
            DataFormat::Csv
        };
        Ok(DataSpec {
            source,
            project: (*project).into(),
            dataset: (*dataset).into(),
            table: (*table).into(),
            fmt,
        })
    }
}

/// A local file's place in the output tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Classified {
    /// Resource directory (``views``, ``models``, ``routines``, ``saved_queries``).
    pub resource_type: String,
    /// Dataset (empty for project-level saved queries).
    pub dataset: String,
    /// Resource name (file stem).
    pub name: String,
}

/// A local file with its place in the output tree, classified once and
/// reused by every later step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassifiedFile {
    /// Resolved absolute path.
    pub path: PathBuf,
    /// Path relative to the output root.
    pub rel: PathBuf,
    pub class: Classified,
}

impl ClassifiedFile {
    /// Directory of the dataset holding the file; ``None`` for project-level
    /// files.
    fn dataset_dir(&self) -> Option<&Path> {
        if self.class.dataset.is_empty() {
            return None;
        }
        self.path.parent()?.parent()
    }
}

/// Classify *path* by its position under *root*; ``Err(path)`` when it is
/// not inside a recognised resource directory.
fn classify_under(path: PathBuf, root: &Path) -> std::result::Result<ClassifiedFile, PathBuf> {
    let Ok(rel) = path.strip_prefix(root).map(Path::to_path_buf) else {
        return Err(path);
    };
    let parts: Vec<String> = rel
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect();
    let class = match parts.as_slice() {
        [dataset, rtype, filename] if KNOWN_DIRS.contains(&rtype.as_str()) => Classified {
            resource_type: rtype.clone(),
            dataset: dataset.clone(),
            name: stem(filename),
        },
        [dir, filename] if PROJECT_DIRS.contains(&dir.as_str()) => Classified {
            resource_type: dir.clone(),
            dataset: String::new(),
            name: stem(filename),
        },
        _ => return Err(path),
    };
    Ok(ClassifiedFile { path, rel, class })
}

/// Resolve and classify *files* under *output_root*.  Returns the
/// classified files and the resolved paths outside any recognised resource
/// directory.
pub fn classify_files(
    files: &[PathBuf],
    output_root: &Path,
) -> (Vec<ClassifiedFile>, Vec<PathBuf>) {
    let root = absolute_resolved(output_root);
    let (mut classified, mut unclassified) = (Vec::new(), Vec::new());
    for file in files {
        match classify_under(absolute_resolved(file), &root) {
            Ok(item) => {
                log::debug!("  classify {} -> {:?}", item.path.display(), item.class);
                classified.push(item);
            }
            Err(path) => {
                log::debug!("  classify {} -> None", path.display());
                unclassified.push(path);
            }
        }
    }
    (classified, unclassified)
}

/// Classify the files named on the command line, warning about those
/// outside a recognised resource directory.
fn classify_args(paths: &[String], output_root: &Path) -> Vec<ClassifiedFile> {
    let files: Vec<PathBuf> = paths.iter().map(PathBuf::from).collect();
    let (classified, unclassified) = classify_files(&files, output_root);
    for f in &unclassified {
        log::warn!("Cannot classify, will skip: {}", f.display());
    }
    classified
}

fn suffix(file: &Path) -> &str {
    file.extension()
        .and_then(|e| e.to_str())
        .unwrap_or_default()
}

/// Push a single classified local file to BigQuery.
pub fn push_file(
    api: &dyn BqApi,
    item: &ClassifiedFile,
    project: &str,
    region: &str,
) -> Result<()> {
    let (file, c) = (item.path.as_path(), &item.class);
    let ds = c.dataset.as_str();
    match (c.resource_type.as_str(), suffix(file)) {
        (VIEW_DIR, "sql") => {
            let u = readers::read_view_sql(file)?;
            api.upsert_view(project, ds, &u.name, &u.sql)
        }
        (VIEW_DIR | MODEL_DIR, "yaml") => {
            let u = readers::read_model_yaml(file)?;
            api.upsert_table_description(
                project,
                ds,
                &u.name,
                &u.description,
                &u.field_descriptions,
            )
        }
        (ROUTINE_DIR, "sql") => {
            let u = readers::read_routine_sql(file)?;
            // The create-fallback path needs the signature from the model YAML.
            let model_yaml = item
                .dataset_dir()
                .map(|d| d.join(MODEL_DIR).join(format!("{}.yaml", c.name)))
                .filter(|p| p.is_file());
            let model = match model_yaml {
                Some(p) => Some(readers::read_routine_model_yaml(&p)?),
                None => None,
            };
            let definition = RoutineDefinition {
                body: &u.body,
                language: &u.language,
                arguments: model.as_ref().map(|m| m.arguments.as_slice()),
                return_type: model.as_ref().and_then(|m| m.return_type.as_deref()),
            };
            api.upsert_routine(project, ds, &u.name, &definition)
        }
        (ROUTINE_DIR, "yaml") => {
            let u = readers::read_model_yaml(file)?;
            api.upsert_routine_description(project, ds, &u.name, &u.description)
        }
        (SAVED_QUERY_DIR, "sql") => {
            let u = readers::read_saved_query_sql(file)?;
            api.upsert_saved_query(project, region, &u.name, &u.sql)
        }
        (SCHEDULED_QUERY_DIR, "sql") => {
            let u = readers::read_scheduled_query_sql(file)?;
            api.upsert_scheduled_query_sql(project, region, &u.name, &u.sql)
        }
        _ => {
            log::warn!(
                "Unsupported resource type/extension, skipping: {}",
                file.display()
            );
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// Changeset detection
// ---------------------------------------------------------------------------

fn git_stdout(args: &[&str], cwd: &Path) -> Option<String> {
    let out = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Uncommitted (staged + unstaged) changed files under *output_root*, or
/// ``None`` when git is unavailable / not a repository.
pub fn git_changed_files(output_root: &Path) -> Option<Vec<PathBuf>> {
    let work_dir = if output_root.is_dir() {
        output_root
    } else {
        output_root.parent().unwrap_or(Path::new("."))
    };
    let repo_root = PathBuf::from(git_stdout(&["rev-parse", "--show-toplevel"], work_dir)?.trim());
    log::debug!("Git repo root: {}", repo_root.display());
    let root_arg = output_root.to_string_lossy();
    let unstaged = git_stdout(&["diff", "--name-only", "--", &root_arg], work_dir)?;
    let staged = git_stdout(
        &["diff", "--name-only", "--cached", "--", &root_arg],
        work_dir,
    )?;
    log::debug!("git diff raw output:\n{unstaged}{staged}");

    let mut seen = HashSet::new();
    let mut files = Vec::new();
    for line in unstaged.lines().chain(staged.lines()) {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let p = repo_root.join(line);
        let is_file = p.is_file();
        log::debug!("  path={line:?}  joined={}  is_file={is_file}", p.display());
        if is_file && seen.insert(p.clone()) {
            files.push(p);
        }
    }
    Some(files)
}

/// Files under *output_root* modified within the last *since_hours* hours.
pub fn mtime_changed_files(output_root: &Path, since_hours: f64) -> Vec<PathBuf> {
    let cutoff = SystemTime::now()
        .checked_sub(Duration::from_secs_f64((since_hours * 3600.0).max(0.0)))
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let mut files = Vec::new();
    let mut stack = vec![output_root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                stack.push(path);
            } else if meta.modified().is_ok_and(|m| m >= cutoff) {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

/// Whether *item* is a model YAML without a companion view / routine SQL
/// (i.e. a table or external table).
pub fn is_materialized_model(item: &ClassifiedFile) -> bool {
    let c = &item.class;
    if c.resource_type != MODEL_DIR || suffix(&item.path) != "yaml" {
        return false;
    }
    let Some(ds_dir) = item.dataset_dir() else {
        return false;
    };
    let sql = format!("{}.sql", c.name);
    !ds_dir.join(VIEW_DIR).join(&sql).is_file() && !ds_dir.join(ROUTINE_DIR).join(&sql).is_file()
}

/// Drop materialized model YAMLs unless *include_models*.
pub fn filter_auto_pushable(
    files: Vec<ClassifiedFile>,
    include_models: bool,
) -> Vec<ClassifiedFile> {
    if include_models {
        return files;
    }
    files
        .into_iter()
        .filter(|f| {
            let skip = is_materialized_model(f);
            if skip {
                log::debug!("  skip materialized model {}", f.path.display());
            }
            !skip
        })
        .collect()
}

/// Sort key: SQL before YAML so structure exists before descriptions.
fn push_order(path: &Path) -> u8 {
    match suffix(path) {
        "sql" => 0,
        "yaml" => 1,
        _ => 2,
    }
}

fn sorted_for_push(files: &[ClassifiedFile]) -> Vec<&ClassifiedFile> {
    let mut sorted: Vec<_> = files.iter().collect();
    sorted.sort_by_key(|f| push_order(&f.path));
    sorted
}

/// Print a tagged changeset to stdout.
pub fn display_changeset(header: &str, files: &[ClassifiedFile], extra_items: &[String]) {
    println!("\n  {header}:\n");
    for f in files {
        println!("    [{}] {}", f.class.resource_type, f.rel.display());
    }
    for item in extra_items {
        println!("    {item}");
    }
    println!();
}

// ---------------------------------------------------------------------------
// Public orchestrators
// ---------------------------------------------------------------------------

/// Options for [`push_manual`] / [`push_auto`] / [`rm_resources`].
#[derive(Debug, Clone)]
pub struct PushOptions {
    pub dry_run: bool,
    /// Skip the interactive confirmation.
    pub yes: bool,
    /// Auto mode: mtime look-back window in hours.
    pub since_hours: f64,
    /// Auto mode: include materialized model YAMLs.
    pub include_models: bool,
    /// Auto mode: use mtime detection instead of git.
    pub use_mtime: bool,
}

impl Default for PushOptions {
    fn default() -> Self {
        PushOptions {
            dry_run: false,
            yes: false,
            since_hours: 24.0,
            include_models: false,
            use_mtime: false,
        }
    }
}

/// Push explicitly listed files and/or replace a table's data.
pub fn push_manual(
    api: &dyn BqApi,
    config: &SyncConfig,
    config_path: &Path,
    paths: &[String],
    data_spec: Option<&DataSpec>,
    opts: &PushOptions,
) -> Result<()> {
    let output_root = resolve_output_dir(config, config_path);
    let pushable = classify_args(paths, &output_root);

    let extra: Vec<String> = data_spec
        .map(|d| {
            format!(
                "[table-replace] {} -> {}/{}/{}",
                d.source.display(),
                d.project,
                d.dataset,
                d.table
            )
        })
        .into_iter()
        .collect();

    if pushable.is_empty() && extra.is_empty() {
        log::info!("Nothing to push.");
        return Ok(());
    }
    display_changeset("Files to push", &pushable, &extra);

    if !proceed(
        opts,
        "Dry-run: no changes written.",
        "Proceed with push?",
        "Push cancelled.",
    ) {
        return Ok(());
    }

    push_all(api, config, &pushable)?;
    if let Some(d) = data_spec {
        api.load_table_from_file(&d.project, &d.dataset, &d.table, &d.source, d.fmt)?;
    }
    log::info!("Push complete.");
    log::info!("Hint: commit pushed changes with 'git commit'.");
    Ok(())
}

/// Detect changed files and push them after confirmation.
pub fn push_auto(
    api: &dyn BqApi,
    config: &SyncConfig,
    config_path: &Path,
    opts: &PushOptions,
) -> Result<()> {
    let output_root = resolve_output_dir(config, config_path);
    if !output_root.exists() {
        log::error!("Output root does not exist: {}", output_root.display());
        return Err(Error::Exit(1));
    }

    let mtime_label = || format!("mtime (last {}h)", opts.since_hours);
    let (candidates, source_label) = if opts.use_mtime {
        (
            mtime_changed_files(&output_root, opts.since_hours),
            mtime_label(),
        )
    } else if let Some(git_files) = git_changed_files(&output_root) {
        (git_files, "git status".to_owned())
    } else {
        log::info!(
            "Git not available, falling back to mtime (last {:.1} hours).",
            opts.since_hours
        );
        (
            mtime_changed_files(&output_root, opts.since_hours),
            mtime_label(),
        )
    };
    let (files, _) = classify_files(&candidates, &output_root);
    let files = filter_auto_pushable(files, opts.include_models);

    if files.is_empty() {
        log::info!("No changed files detected via {source_label}.");
        return Ok(());
    }
    log::info!(
        "Detected {} changed file(s) via {source_label}.",
        files.len()
    );

    if opts.dry_run {
        display_changeset("Files that would be pushed", &files, &[]);
    } else if !opts.yes {
        display_changeset("Files to push", &files, &[]);
    }
    if !proceed(
        opts,
        "Dry-run: no changes written.",
        "Proceed with push?",
        "Push cancelled.",
    ) {
        return Ok(());
    }

    push_all(api, config, &files)?;
    log::info!("Push complete.");
    log::info!("Hint: commit pushed changes with 'git commit'.");
    Ok(())
}

/// Whether to carry out the changeset already shown: ``false`` for a dry
/// run (reported with *dry_run_msg*) or when the user declines *prompt*
/// (reported with *cancelled_msg*).
fn proceed(opts: &PushOptions, dry_run_msg: &str, prompt: &str, cancelled_msg: &str) -> bool {
    if opts.dry_run {
        log::info!("{dry_run_msg}");
        return false;
    }
    if !opts.yes && !confirm(prompt) {
        log::info!("{cancelled_msg}");
        return false;
    }
    true
}

/// Push *files* in dependency order (SQL before YAML).
fn push_all(api: &dyn BqApi, config: &SyncConfig, files: &[ClassifiedFile]) -> Result<()> {
    for f in sorted_for_push(files) {
        push_file(api, f, &config.project.id, &config.project.default_region)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// rm — delete the BQ resource, then the local file
// ---------------------------------------------------------------------------

/// Delete the BigQuery resource identified by *item*, then its file.
pub fn rm_file(api: &dyn BqApi, item: &ClassifiedFile, project: &str, region: &str) -> Result<()> {
    let (file, c) = (item.path.as_path(), &item.class);
    let ds = c.dataset.as_str();
    match (c.resource_type.as_str(), suffix(file)) {
        (VIEW_DIR, "sql") => api.delete_view(project, ds, &c.name)?,
        (VIEW_DIR, "yaml") => {
            let u = readers::read_view_model_yaml(file)?;
            api.delete_view(project, ds, &u.name)?;
        }
        (MODEL_DIR, "yaml") => {
            let u = readers::read_model_yaml(file)?;
            api.delete_table(project, ds, &u.name)?;
        }
        (ROUTINE_DIR, "sql") => {
            let u = readers::read_routine_sql(file)?;
            api.delete_routine(project, ds, &u.name)?;
        }
        (SAVED_QUERY_DIR, "sql") => {
            let u = readers::read_saved_query_sql(file)?;
            api.delete_saved_query(project, region, &u.name)?;
        }
        _ => {
            log::warn!(
                "Unsupported resource type/extension for rm, skipping: {}",
                file.display()
            );
            return Ok(());
        }
    }
    std::fs::remove_file(file).map_err(|e| Error::io(file, e))?;
    log::info!("Deleted local file {}", file.display());
    Ok(())
}

/// Delete BigQuery resources identified by local file paths.
pub fn rm_resources(
    api: &dyn BqApi,
    config: &SyncConfig,
    config_path: &Path,
    paths: &[String],
    opts: &PushOptions,
) -> Result<()> {
    let output_root = resolve_output_dir(config, config_path);
    let (scheduled, removable): (Vec<_>, Vec<_>) = classify_args(paths, &output_root)
        .into_iter()
        .partition(|f| f.class.resource_type == SCHEDULED_QUERY_DIR);
    for f in &scheduled {
        log::warn!(
            "rm does not delete scheduled queries, skipping: {}",
            f.path.display()
        );
    }

    if removable.is_empty() {
        log::info!("Nothing to remove.");
        return Ok(());
    }
    display_changeset("Resources to delete (BQ + local)", &removable, &[]);

    if !proceed(
        opts,
        "Dry-run: no resources deleted.",
        "Proceed with deletion?",
        "Deletion cancelled.",
    ) {
        return Ok(());
    }
    for f in &removable {
        rm_file(api, f, &config.project.id, &config.project.default_region)?;
    }
    log::info!("Deletion complete.");
    log::info!("Hint: commit deletions with 'git add -A && git commit'.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fetch::tests::{git, init_repo};
    use crate::testing::FakeApi;

    fn config() -> SyncConfig {
        SyncConfig::from_toml(
            "[project]\nid = \"my-project\"\ndefault_region = \"us-east1\"\n[sync]\ndatasets = [\"my_dataset\"]\n",
        )
        .unwrap()
    }

    /// Returns (config_path, output_root).
    fn tree(base: &Path) -> (PathBuf, PathBuf) {
        let base = base.canonicalize().unwrap();
        let root = base.join("my-project");
        for d in [
            "my_dataset/views",
            "my_dataset/models",
            "my_dataset/routines",
            "saved_queries",
            "scheduled_queries",
        ] {
            std::fs::create_dir_all(root.join(d)).unwrap();
        }
        (base.join("bq_sync.toml"), root)
    }

    fn write(p: &Path, s: &str) -> PathBuf {
        std::fs::write(p, s).unwrap();
        p.to_path_buf()
    }

    fn cls(t: &str, d: &str, n: &str) -> Option<Classified> {
        Some(Classified {
            resource_type: t.into(),
            dataset: d.into(),
            name: n.into(),
        })
    }

    fn classify_path(file: &Path, root: &Path) -> Option<Classified> {
        classify_files(&[file.to_path_buf()], root)
            .0
            .pop()
            .map(|f| f.class)
    }

    #[test]
    fn classify() {
        let dir = tempfile::tempdir().unwrap();
        let (_, root) = tree(dir.path());
        let f = write(&root.join("my_dataset/views/active.sql"), "SELECT 1");
        assert_eq!(
            classify_path(&f, &root),
            cls("views", "my_dataset", "active")
        );
        let f = write(&root.join("my_dataset/models/users.yaml"), "name: users");
        assert_eq!(
            classify_path(&f, &root),
            cls("models", "my_dataset", "users")
        );
        let f = write(&root.join("saved_queries/my_query.sql"), "SELECT 1");
        assert_eq!(
            classify_path(&f, &root),
            cls("saved_queries", "", "my_query")
        );
        let f = write(&root.join("scheduled_queries/daily_load.sql"), "SELECT 1");
        assert_eq!(
            classify_path(&f, &root),
            cls("scheduled_queries", "", "daily_load")
        );
        let f = write(&root.join("my_dataset/routines/fn.sql"), "x");
        assert_eq!(
            classify_path(&f, &root),
            cls("routines", "my_dataset", "fn")
        );
        std::fs::create_dir_all(root.join("my_dataset/other")).unwrap();
        let f = write(&root.join("my_dataset/other/x.sql"), "x");
        assert_eq!(classify_path(&f, &root), None);
        let outside = write(&dir.path().join("outside.sql"), "x");
        assert_eq!(classify_path(&outside, &root), None);
    }

    #[test]
    fn filters() {
        let dir = tempfile::tempdir().unwrap();
        let (_, root) = tree(dir.path());
        let view = write(&root.join("my_dataset/views/v.sql"), "SELECT 1");
        let unknown = write(&root.join("README.md"), "x");
        let (kept, skipped) = classify_files(&[view.clone(), unknown.clone()], &root);
        assert_eq!(kept.iter().map(|f| &f.path).collect::<Vec<_>>(), [&view]);
        assert_eq!(skipped, vec![unknown]);
        assert_eq!(kept[0].rel, Path::new("my_dataset/views/v.sql"));

        let table = write(&root.join("my_dataset/models/t.yaml"), "name: t");
        let v_model = write(&root.join("my_dataset/models/v.yaml"), "name: v");
        write(&root.join("my_dataset/routines/fn.sql"), "x");
        let fn_model = write(&root.join("my_dataset/models/fn.yaml"), "name: fn");
        let classify_all = |paths: &[&PathBuf]| {
            let paths: Vec<PathBuf> = paths.iter().map(|p| (*p).clone()).collect();
            classify_files(&paths, &root).0
        };
        let models = classify_all(&[&table, &v_model, &fn_model, &view]);
        let flags: Vec<bool> = models.iter().map(is_materialized_model).collect();
        assert_eq!(flags, [true, false, false, false]);

        let all = classify_all(&[&table, &v_model]);
        assert_eq!(
            filter_auto_pushable(all.clone(), false),
            vec![all[1].clone()]
        );
        assert_eq!(filter_auto_pushable(all.clone(), true), all);
    }

    #[test]
    fn mtime_detection() {
        let dir = tempfile::tempdir().unwrap();
        let (_, root) = tree(dir.path());
        let f = write(&root.join("my_dataset/views/v.sql"), "SELECT 1");
        assert_eq!(mtime_changed_files(&root, 1.0), vec![f]);
        std::thread::sleep(Duration::from_millis(20));
        assert!(mtime_changed_files(&root, 0.0).is_empty());
    }

    #[test]
    fn manual_dry_run_makes_no_calls() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, root) = tree(dir.path());
        let f = write(&root.join("my_dataset/views/v.sql"), "SELECT 1");
        let api = FakeApi::default();
        let opts = PushOptions {
            dry_run: true,
            ..Default::default()
        };
        push_manual(
            &api,
            &config(),
            &cfg,
            &[f.to_string_lossy().into()],
            None,
            &opts,
        )
        .unwrap();
        assert!(api.calls().is_empty());
    }

    #[test]
    fn manual_push_dispatch_and_order() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, root) = tree(dir.path());
        let model = write(
            &root.join("my_dataset/models/v.yaml"),
            "name: v\ndescription: \"Doc\"\nschema:\n  - name: id  type: INT64  mode: NULLABLE  description: \"Id\"\n",
        );
        let view = write(&root.join("my_dataset/views/v.sql"), "SELECT 1");
        let routine = write(
            &root.join("my_dataset/routines/fn.sql"),
            "-- Routine: fn\n-- Language: SQL\n\nx + 1",
        );
        write(
            &root.join("my_dataset/models/fn.yaml"),
            "name: fn\ndescription: \"\"\nlanguage: SQL\nreturn_type: INT64\narguments:\n  - name: x  type: INT64  mode: IN\n",
        );
        let saved = write(
            &root.join("saved_queries/q.sql"),
            "-- Saved Query: Q\n\nSELECT 2",
        );
        let api = FakeApi::default();
        let opts = PushOptions {
            yes: true,
            ..Default::default()
        };
        let paths: Vec<String> = [&model, &view, &routine, &saved]
            .iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect();
        push_manual(&api, &config(), &cfg, &paths, None, &opts).unwrap();
        assert_eq!(
            api.calls(),
            vec![
                "upsert_view my-project.my_dataset.v SELECT 1",
                "upsert_routine my-project.my_dataset.fn SQL args=[x:INT64] ret=Some(\"INT64\")",
                "upsert_saved_query my-project/us-east1 Q",
                "upsert_table_description my-project.my_dataset.v Doc fields=1",
            ]
        );
    }

    #[test]
    fn manual_push_data_spec() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, _) = tree(dir.path());
        let src = write(&dir.path().join("data.parquet"), "x");
        let spec = DataSpec::parse(&src.to_string_lossy(), "p/d/t").unwrap();
        assert_eq!(spec.fmt, DataFormat::Parquet);
        assert!(matches!(
            DataSpec::parse("a.csv", "p/d"),
            Err(Error::Exit(1))
        ));
        let api = FakeApi::default();
        let opts = PushOptions {
            yes: true,
            ..Default::default()
        };
        push_manual(&api, &config(), &cfg, &[], Some(&spec), &opts).unwrap();
        assert_eq!(api.calls(), vec!["load_table_from_file p.d.t Parquet"]);
    }

    #[test]
    fn auto_mode_git_and_mtime() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, root) = tree(dir.path());
        let api = FakeApi::default();
        let opts = PushOptions {
            yes: true,
            ..Default::default()
        };

        // No git repository → mtime fallback picks up the new file.
        let view = write(&root.join("my_dataset/views/v.sql"), "SELECT 1");
        push_auto(&api, &config(), &cfg, &opts).unwrap();
        assert_eq!(api.calls().len(), 1);

        // Git repository with a committed file → only modified files count.
        init_repo(dir.path());
        git(dir.path(), &["add", "-A"]);
        git(dir.path(), &["commit", "-qm", "init"]);
        let api = FakeApi::default();
        push_auto(&api, &config(), &cfg, &opts).unwrap();
        assert!(api.calls().is_empty());
        write(&view, "SELECT 2");
        push_auto(&api, &config(), &cfg, &opts).unwrap();
        assert_eq!(
            api.calls(),
            vec!["upsert_view my-project.my_dataset.v SELECT 2"]
        );

        // Explicit mtime mode skips git.
        let api = FakeApi::default();
        let opts = PushOptions {
            dry_run: true,
            use_mtime: true,
            since_hours: 1.0,
            ..Default::default()
        };
        push_auto(&api, &config(), &cfg, &opts).unwrap();
        assert!(api.calls().is_empty());
    }

    #[test]
    fn scheduled_query_push_updates_sql_only() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, root) = tree(dir.path());
        let sq = write(
            &root.join("scheduled_queries/daily_load.sql"),
            "-- Scheduled Query: daily_load\n-- Schedule: every 24 hours\n\nCREATE OR REPLACE TABLE `p.d.t` AS SELECT 1\n",
        );
        let missing = write(
            &root.join("scheduled_queries/gone.sql"),
            "-- Scheduled Query: gone\n-- Schedule: every 24 hours\n\nSELECT 1\n",
        );
        let api = FakeApi::default();
        *api.transfer_configs.borrow_mut() = vec![crate::resources::TransferConfig {
            name: "projects/my-project/locations/us-east1/transferConfigs/7".into(),
            display_name: "daily_load".into(),
            data_source_id: "scheduled_query".into(),
            ..Default::default()
        }];
        let opts = PushOptions {
            yes: true,
            ..Default::default()
        };
        let paths = [&sq, &missing].map(|p| p.to_string_lossy().into_owned());
        push_manual(&api, &config(), &cfg, &paths, None, &opts).unwrap();
        // Only the existing query is updated; the missing one is skipped
        // (no create path) and the schedule header is not pushed.
        assert_eq!(
            api.calls(),
            vec![
                "update_query projects/my-project/locations/us-east1/transferConfigs/7 \
                 CREATE OR REPLACE TABLE `p.d.t` AS SELECT 1\n"
            ]
        );
    }

    #[test]
    fn duplicate_scheduled_query_names_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, root) = tree(dir.path());
        let sq = write(
            &root.join("scheduled_queries/daily_load.sql"),
            "-- Scheduled Query: daily_load\n\nSELECT 1\n",
        );
        let api = FakeApi::default();
        *api.transfer_configs.borrow_mut() = ["1", "2"]
            .map(|id| crate::resources::TransferConfig {
                name: format!("projects/p/locations/l/transferConfigs/{id}"),
                display_name: "daily_load".into(),
                data_source_id: "scheduled_query".into(),
                ..Default::default()
            })
            .to_vec();
        let opts = PushOptions {
            yes: true,
            ..Default::default()
        };
        let paths = [sq.to_string_lossy().into_owned()];
        let err = push_manual(&api, &config(), &cfg, &paths, None, &opts).unwrap_err();
        assert!(matches!(err, Error::Invalid(m) if m.contains("Several scheduled queries")));
        assert!(api.calls().is_empty());
    }

    #[test]
    fn rm_skips_scheduled_queries() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, root) = tree(dir.path());
        let sq = write(
            &root.join("scheduled_queries/daily_load.sql"),
            "-- Scheduled Query: daily_load\n\nSELECT 1\n",
        );
        let api = FakeApi::default();
        let yes = PushOptions {
            yes: true,
            ..Default::default()
        };
        let paths = [sq.to_string_lossy().into_owned()];
        rm_resources(&api, &config(), &cfg, &paths, &yes).unwrap();
        assert!(api.calls().is_empty() && sq.exists());
    }

    #[test]
    fn auto_missing_root_exits() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join("bq_sync.toml");
        let err = push_auto(
            &FakeApi::default(),
            &config(),
            &cfg,
            &PushOptions::default(),
        );
        assert!(matches!(err, Err(Error::Exit(1))));
    }

    #[test]
    fn rm_deletes_remote_then_local() {
        let dir = tempfile::tempdir().unwrap();
        let (cfg, root) = tree(dir.path());
        let view = write(&root.join("my_dataset/views/v.sql"), "SELECT 1");
        let model = write(&root.join("my_dataset/models/t.yaml"), "name: t\n");
        let paths = vec![
            view.to_string_lossy().into_owned(),
            model.to_string_lossy().into_owned(),
        ];

        let api = FakeApi::default();
        let dry = PushOptions {
            dry_run: true,
            ..Default::default()
        };
        rm_resources(&api, &config(), &cfg, &paths, &dry).unwrap();
        assert!(api.calls().is_empty() && view.exists());

        let yes = PushOptions {
            yes: true,
            ..Default::default()
        };
        rm_resources(&api, &config(), &cfg, &paths, &yes).unwrap();
        assert_eq!(
            api.calls(),
            vec![
                "delete_view my-project.my_dataset.v",
                "delete_table my-project.my_dataset.t"
            ]
        );
        assert!(!view.exists() && !model.exists());
    }
}
