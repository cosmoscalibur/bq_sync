//! Fetch decision logic for pull operations.
//!
//! Compares BigQuery modification times with git commit timestamps to
//! decide whether each resource is fetched, skipped, or warned about.

use std::path::Path;
use std::process::Command;

use chrono::{DateTime, Utc};

/// Outcome of the fetch decision matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchAction {
    Fetch,
    Skip,
    Warn,
}

fn git_output(args: &[&str], cwd: &Path) -> Option<String> {
    let out = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).into_owned())
}

/// Last git commit time for *file*, or ``None`` without history.
pub fn git_committed_time(file: &Path) -> Option<DateTime<Utc>> {
    let cwd = file.parent().unwrap_or(Path::new("."));
    let file_arg = file.to_string_lossy();
    let raw = git_output(&["log", "-1", "--format=%cI", "--", &file_arg], cwd)?;
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

/// Whether *output_root* contains uncommitted changes (``git status``).
pub fn has_uncommitted_changes(output_root: &Path) -> bool {
    let cwd = if output_root.is_dir() {
        output_root
    } else {
        output_root.parent().unwrap_or(Path::new("."))
    };
    let root_arg = output_root.to_string_lossy();
    git_output(&["status", "--porcelain", "--", &root_arg], cwd)
        .is_some_and(|out| !out.trim().is_empty())
}

/// Apply the fetch decision matrix.
///
/// | BQ exists | File exists | Git history | Condition         | Action |
/// |-----------|-------------|-------------|-------------------|--------|
/// | Yes       | No          | —           | —                 | Fetch  |
/// | Yes       | Yes         | No          | —                 | Warn   |
/// | Yes       | Yes         | Yes         | BQ ≤ committed    | Skip   |
/// | Yes       | Yes         | Yes         | BQ > committed    | Fetch  |
/// | No        | No          | —           | —                 | Skip   |
/// | No        | Yes         | —           | —                 | Warn   |
///
/// ``force`` bypasses the matrix and always fetches.
pub fn resolve(bq_modified: Option<DateTime<Utc>>, file: &Path, force: bool) -> FetchAction {
    if force {
        return FetchAction::Fetch;
    }
    let file_exists = file.is_file();
    let Some(bq_modified) = bq_modified else {
        if file_exists {
            log::warn!(
                "Resource deleted on BQ but local file exists: {}",
                file.display()
            );
            return FetchAction::Warn;
        }
        return FetchAction::Skip;
    };
    if !file_exists {
        return FetchAction::Fetch;
    }
    let Some(committed) = git_committed_time(file) else {
        log::warn!(
            "Local file not committed, cannot compare: {}",
            file.display()
        );
        return FetchAction::Warn;
    };
    if bq_modified <= committed {
        return FetchAction::Skip;
    }
    log::warn!(
        "BQ resource is more recent than committed version: {} (BQ: {}, committed: {})",
        file.display(),
        crate::utils::isoformat(&bq_modified),
        crate::utils::isoformat(&committed),
    );
    FetchAction::Fetch
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use chrono::{Duration, TimeZone};
    use std::path::PathBuf;

    pub(crate) fn git(dir: &Path, args: &[&str]) {
        let status = Command::new("git")
            .args(args)
            .current_dir(dir)
            .env("GIT_AUTHOR_NAME", "t")
            .env("GIT_AUTHOR_EMAIL", "t@t")
            .env("GIT_COMMITTER_NAME", "t")
            .env("GIT_COMMITTER_EMAIL", "t@t")
            .output()
            .unwrap()
            .status;
        assert!(status.success(), "git {args:?} failed");
    }

    pub(crate) fn init_repo(dir: &Path) {
        git(dir, &["init", "-q"]);
        git(dir, &["config", "commit.gpgsign", "false"]);
    }

    fn committed_file(dir: &Path) -> PathBuf {
        init_repo(dir);
        let f = dir.join("a.sql");
        std::fs::write(&f, "SELECT 1").unwrap();
        git(dir, &["add", "a.sql"]);
        git(dir, &["commit", "-qm", "init"]);
        f
    }

    #[test]
    fn untracked_has_no_history() {
        let dir = tempfile::tempdir().unwrap();
        init_repo(dir.path());
        let f = dir.path().join("new.sql");
        std::fs::write(&f, "x").unwrap();
        assert!(git_committed_time(&f).is_none());
    }

    #[test]
    fn committed_time_is_utc_now() {
        let dir = tempfile::tempdir().unwrap();
        let f = committed_file(dir.path());
        let t = git_committed_time(&f).unwrap();
        assert!((Utc::now() - t).num_seconds().abs() < 120);
    }

    #[test]
    fn uncommitted_detection() {
        let dir = tempfile::tempdir().unwrap();
        let f = committed_file(dir.path());
        assert!(!has_uncommitted_changes(dir.path()));
        std::fs::write(&f, "SELECT 2").unwrap();
        assert!(has_uncommitted_changes(dir.path()));
    }

    #[test]
    fn decision_matrix() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing.sql");
        let old = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap();

        assert_eq!(resolve(Some(old), &missing, false), FetchAction::Fetch);
        assert_eq!(resolve(None, &missing, false), FetchAction::Skip);
        assert_eq!(resolve(None, &missing, true), FetchAction::Fetch);

        let f = committed_file(dir.path());
        assert_eq!(resolve(Some(old), &f, false), FetchAction::Skip);
        let future = Utc::now() + Duration::days(1);
        assert_eq!(resolve(Some(future), &f, false), FetchAction::Fetch);
        assert_eq!(resolve(None, &f, false), FetchAction::Warn);
        assert_eq!(resolve(Some(old), &f, true), FetchAction::Fetch);

        let untracked = dir.path().join("untracked.sql");
        std::fs::write(&untracked, "x").unwrap();
        assert_eq!(resolve(Some(old), &untracked, false), FetchAction::Warn);
    }
}
