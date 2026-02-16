"""TOML configuration loading and config file discovery."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path

CONFIG_FILENAME = "bq_sync.toml"


@dataclass(frozen=True)
class ProjectConfig:
    """GCP project configuration."""

    id: str
    default_region: str


@dataclass(frozen=True)
class SyncConfig:
    """Top-level sync configuration parsed from ``bq_sync.toml``."""

    project: ProjectConfig
    datasets: list[str]
    output_dir: str


def load_config(path: Path) -> SyncConfig:
    """Read and parse a ``bq_sync.toml`` file.

    Args:
        path: Absolute or relative path to the TOML config file.

    Returns:
        Parsed ``SyncConfig``.

    Raises:
        FileNotFoundError: If *path* does not exist.
        KeyError: If required keys are missing from the TOML.
    """
    with path.open("rb") as fh:
        raw = tomllib.load(fh)

    project_raw = raw["project"]
    project = ProjectConfig(
        id=project_raw["id"],
        default_region=project_raw["default_region"],
    )

    sync_raw = raw["sync"]
    return SyncConfig(
        project=project,
        datasets=list(sync_raw["datasets"]),
        output_dir=sync_raw.get("output_dir", "."),
    )


def discover_config(start: Path | None = None) -> Path:
    """Walk from *start* upward looking for ``bq_sync.toml``.

    Args:
        start: Directory to begin the search.  Defaults to the current
            working directory.

    Returns:
        Absolute path to the discovered config file.

    Raises:
        FileNotFoundError: If no ``bq_sync.toml`` is found between *start*
            and the filesystem root.
    """
    current = (start or Path.cwd()).resolve()

    while True:
        candidate = current / CONFIG_FILENAME
        if candidate.is_file():
            return candidate

        parent = current.parent
        if parent == current:
            break
        current = parent

    msg = f"{CONFIG_FILENAME} not found (searched from {start or Path.cwd()})"
    raise FileNotFoundError(msg)


def resolve_output_dir(config: SyncConfig, config_path: Path) -> Path:
    """Resolve the output directory for synced files.

    The output directory is resolved relative to the config file's parent,
    then ``/<project_id>/`` is appended to isolate multiple projects.

    Args:
        config: Parsed sync configuration.
        config_path: Path to the ``bq_sync.toml`` that was loaded.

    Returns:
        Absolute path to the project-scoped output directory.
    """
    base = config_path.resolve().parent / config.output_dir
    return (base / config.project.id).resolve()
