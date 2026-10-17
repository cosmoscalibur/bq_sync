//! TOML configuration loading and config file discovery.

use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::error::{Error, Result};

/// Name of the configuration file searched for by [`discover_config`].
pub const CONFIG_FILENAME: &str = "bq_sync.toml";

/// GCP project configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ProjectConfig {
    pub id: String,
    pub default_region: String,
}

/// Top-level sync configuration parsed from ``bq_sync.toml``.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncConfig {
    pub project: ProjectConfig,
    pub datasets: Vec<String>,
    pub output_dir: String,
}

#[derive(Deserialize)]
struct RawSync {
    datasets: Vec<String>,
    #[serde(default = "default_output_dir")]
    output_dir: String,
}

#[derive(Deserialize)]
struct RawConfig {
    project: ProjectConfig,
    sync: RawSync,
}

fn default_output_dir() -> String {
    ".".to_owned()
}

impl SyncConfig {
    /// Parse a configuration from TOML text.
    pub fn from_toml(text: &str) -> Result<Self> {
        let raw: RawConfig =
            toml::from_str(text).map_err(|e| Error::Config(format!("invalid config: {e}")))?;
        Ok(SyncConfig {
            project: raw.project,
            datasets: raw.sync.datasets,
            output_dir: raw.sync.output_dir,
        })
    }

    /// Return a copy restricted to a single dataset.
    pub fn with_datasets(&self, datasets: Vec<String>) -> Self {
        SyncConfig {
            datasets,
            ..self.clone()
        }
    }
}

/// Read and parse a ``bq_sync.toml`` file.
pub fn load_config(path: &Path) -> Result<SyncConfig> {
    let text = std::fs::read_to_string(path).map_err(|e| Error::io(path, e))?;
    SyncConfig::from_toml(&text).map_err(|e| match e {
        Error::Config(msg) => Error::Config(format!("{}: {msg}", path.display())),
        other => other,
    })
}

/// Walk from *start* (default: current directory) upward looking for
/// ``bq_sync.toml``.
pub fn discover_config(start: Option<&Path>) -> Result<PathBuf> {
    let origin = match start {
        Some(p) => p.to_path_buf(),
        None => std::env::current_dir().map_err(|e| Error::io(".", e))?,
    };
    let mut current = std::path::absolute(&origin).map_err(|e| Error::io(&origin, e))?;
    if let Ok(canonical) = current.canonicalize() {
        current = canonical;
    }

    loop {
        let candidate = current.join(CONFIG_FILENAME);
        if candidate.is_file() {
            return Ok(candidate);
        }
        if !current.pop() {
            break;
        }
    }

    Err(Error::Config(format!(
        "{CONFIG_FILENAME} not found (searched from {})",
        origin.display()
    )))
}

/// Resolve the project-scoped output directory:
/// ``<config dir>/<output_dir>/<project_id>``.
pub fn resolve_output_dir(config: &SyncConfig, config_path: &Path) -> PathBuf {
    let config_path = absolute_resolved(config_path);
    let base = config_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_default()
        .join(&config.output_dir);
    absolute_resolved(&base.join(&config.project.id))
}

/// Make *path* absolute and resolve symlinks / ``..`` where possible
/// (non-existent paths are normalised lexically), like ``Path.resolve()``.
pub fn absolute_resolved(path: &Path) -> PathBuf {
    if let Ok(p) = path.canonicalize() {
        return p;
    }
    let abs = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
    // Canonicalise the longest existing ancestor, then append the rest.
    let mut existing = abs.clone();
    let mut tail = Vec::new();
    while !existing.exists() {
        match existing.file_name() {
            Some(name) => {
                tail.push(name.to_os_string());
                existing.pop();
            }
            None => break,
        }
    }
    let mut out = existing.canonicalize().unwrap_or(existing);
    for part in tail.into_iter().rev() {
        out.push(part);
    }
    normalize_lexically(&out)
}

fn normalize_lexically(path: &Path) -> PathBuf {
    use std::path::Component;
    let mut out = PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r#"
[project]
id = "my-project"
default_region = "us-east1"

[sync]
datasets = ["ds_a", "ds_b"]
output_dir = "out"
"#;

    #[test]
    fn load_valid_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(CONFIG_FILENAME);
        std::fs::write(&path, VALID).unwrap();
        let cfg = load_config(&path).unwrap();
        assert_eq!(cfg.project.id, "my-project");
        assert_eq!(cfg.project.default_region, "us-east1");
        assert_eq!(cfg.datasets, vec!["ds_a", "ds_b"]);
        assert_eq!(cfg.output_dir, "out");
    }

    #[test]
    fn missing_key_is_error() {
        let err = SyncConfig::from_toml("[project]\nid = \"x\"\n[sync]\ndatasets = []\n");
        assert!(matches!(err, Err(Error::Config(_))));
    }

    #[test]
    fn empty_datasets_and_default_output_dir() {
        let cfg = SyncConfig::from_toml(
            "[project]\nid = \"p\"\ndefault_region = \"r\"\n[sync]\ndatasets = []\n",
        )
        .unwrap();
        assert!(cfg.datasets.is_empty());
        assert_eq!(cfg.output_dir, ".");
    }

    #[test]
    fn discover_in_start_and_parent() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join(CONFIG_FILENAME);
        std::fs::write(&cfg, VALID).unwrap();
        let child = dir.path().join("a").join("b");
        std::fs::create_dir_all(&child).unwrap();

        let found = discover_config(Some(dir.path())).unwrap();
        assert_eq!(found, cfg.canonicalize().unwrap());
        let found = discover_config(Some(&child)).unwrap();
        assert_eq!(found, cfg.canonicalize().unwrap());
    }

    #[test]
    fn discover_not_found() {
        let dir = tempfile::tempdir().unwrap();
        assert!(matches!(
            discover_config(Some(dir.path())),
            Err(Error::Config(_))
        ));
    }

    #[test]
    fn output_dir_resolution() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().canonicalize().unwrap();
        let path = base.join(CONFIG_FILENAME);
        let cfg = SyncConfig::from_toml(VALID).unwrap();
        assert_eq!(
            resolve_output_dir(&cfg, &path),
            base.join("out").join("my-project")
        );
        let dot = SyncConfig {
            output_dir: ".".into(),
            ..cfg
        };
        assert_eq!(resolve_output_dir(&dot, &path), base.join("my-project"));
    }
}
