"""Push orchestrator — deploy local resources to BigQuery.

Supports two modes:

- **Manual**: push explicitly listed files via ``--path``, and
  optionally replace a table via ``--data``.
- **Auto**: detect changed files (via ``git status`` or file mtime)
  and push them after interactive confirmation.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from bq_sync import bq_client
from bq_sync.config import SyncConfig, resolve_output_dir
from bq_sync.readers import (
    read_model_yaml,
    read_routine_sql,
    read_saved_query_sql,
    read_view_model_yaml,
    read_view_sql,
)

logger = logging.getLogger(__name__)

# Resource directories recognised during path classification.
_VIEW_DIR = "views"
_MODEL_DIR = "models"
_ROUTINE_DIR = "routines"
_SAVED_QUERY_DIR = "saved_queries"

# Directories that can be pushed or removed.
_KNOWN_DIRS = {_VIEW_DIR, _MODEL_DIR, _ROUTINE_DIR, _SAVED_QUERY_DIR}


# ---------------------------------------------------------------------------
# Data-spec for table-replace
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DataSpec:
    """Specification for a table-replace operation.

    Attributes:
        source: Local CSV or Parquet file.
        project: GCP project ID.
        dataset: BigQuery dataset ID.
        table: Target table name.
        fmt: Source format (``"csv"`` or ``"parquet"``).
    """

    source: Path
    project: str
    dataset: str
    table: str
    fmt: str


# ---------------------------------------------------------------------------
# Path classification
# ---------------------------------------------------------------------------


def _classify_path(file: Path, output_root: Path) -> tuple[str, str, str] | None:
    """Classify a local file path into ``(resource_type, dataset, name)``.

    Returns ``None`` when the file does not belong to a recognised
    resource directory.

    Args:
        file: Absolute path to a local resource file.
        output_root: Project-scoped output directory.

    Returns:
        Tuple of resource type, dataset, and resource name, or ``None``.
    """
    try:
        rel = file.resolve().relative_to(output_root.resolve())
    except ValueError:
        return None

    parts = rel.parts
    # Dataset resources: <dataset>/<resource_type>/<name>.ext
    if len(parts) == 3:
        dataset, resource_type, filename = parts
        if resource_type in _KNOWN_DIRS:
            return resource_type, dataset, Path(filename).stem
    # Project-level saved queries: saved_queries/<name>.ext
    if len(parts) == 2 and parts[0] == _SAVED_QUERY_DIR:
        return _SAVED_QUERY_DIR, "", Path(parts[1]).stem
    return None


# ---------------------------------------------------------------------------
# Single-file push dispatcher
# ---------------------------------------------------------------------------


def _push_file(
    file: Path,
    output_root: Path,
    project: str,
    region: str,
) -> None:
    """Push a single local file to BigQuery.

    Dispatches to the appropriate BQ write function based on the
    file's position in the output tree.

    Args:
        file: Absolute path to the resource file.
        output_root: Project-scoped output directory.
        project: GCP project ID.
        region: GCP region.
    """
    classified = _classify_path(file, output_root)
    if classified is None:
        logger.warning("Cannot classify file, skipping: %s", file)
        return

    resource_type, dataset, name = classified

    if resource_type == _VIEW_DIR and file.suffix == ".sql":
        update = read_view_sql(file)
        bq_client.update_view(project, dataset, update.name, update.sql)

    elif resource_type == _VIEW_DIR and file.suffix == ".yaml":
        # View model YAML — push descriptions via the models dir.
        update = read_view_model_yaml(file)
        bq_client.update_table_description(
            project,
            dataset,
            update.name,
            update.description,
            update.field_descriptions or None,
        )

    elif resource_type == _MODEL_DIR and file.suffix == ".yaml":
        update = read_model_yaml(file)
        bq_client.update_table_description(
            project,
            dataset,
            update.name,
            update.description,
            update.field_descriptions or None,
        )

    elif resource_type == _ROUTINE_DIR and file.suffix == ".sql":
        update = read_routine_sql(file)
        bq_client.update_routine(project, dataset, update.name, update.body)

    elif resource_type == _SAVED_QUERY_DIR and file.suffix == ".sql":
        update = read_saved_query_sql(file)
        bq_client.update_saved_query(project, region, update.name, update.sql)

    else:
        logger.warning("Unsupported resource type/extension, skipping: %s", file)


# ---------------------------------------------------------------------------
# Changeset detection
# ---------------------------------------------------------------------------


def _git_changed_files(output_root: Path) -> list[Path] | None:
    """Return uncommitted changed files via ``git status``.

    Returns ``None`` when git is not available or the directory is not
    a git repository.

    Args:
        output_root: Directory to check.

    Returns:
        List of changed file paths, or ``None`` when git is unavailable.
    """
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain", "--", str(output_root)],
            capture_output=True,
            text=True,
            check=True,
            cwd=output_root if output_root.is_dir() else output_root.parent,
        )
    except (subprocess.CalledProcessError, OSError, FileNotFoundError):
        return None

    files: list[Path] = []
    for line in result.stdout.strip().splitlines():
        if not line:
            continue
        # porcelain format: "XY <path>" or "XY <path> -> <path>"
        raw_path = line[3:].split(" -> ")[-1].strip()
        p = (output_root / raw_path).resolve()
        if p.is_file():
            files.append(p)
    return files


def _mtime_changed_files(output_root: Path, since_hours: float) -> list[Path]:
    """Return files modified within the last *since_hours* hours.

    Args:
        output_root: Directory to scan recursively.
        since_hours: Look-back window in hours.

    Returns:
        List of recently modified file paths.
    """
    cutoff = time.time() - since_hours * 3600
    files: list[Path] = []
    for root, _dirs, filenames in os.walk(output_root):
        for fname in filenames:
            p = Path(root) / fname
            if p.stat().st_mtime >= cutoff:
                files.append(p.resolve())
    return files


def _filter_pushable(files: list[Path], output_root: Path) -> list[Path]:
    """Keep only files that belong to a recognised resource directory.

    Args:
        files: Candidate file paths.
        output_root: Project-scoped output directory.

    Returns:
        Filtered list of pushable files.
    """
    return [f for f in files if _classify_path(f, output_root) is not None]


# ---------------------------------------------------------------------------
# Confirmation prompt
# ---------------------------------------------------------------------------


def _confirm_changeset(files: list[Path], output_root: Path) -> bool:
    """Print a changeset report and prompt the user for confirmation.

    Args:
        files: Files that will be pushed.
        output_root: Project-scoped output directory.

    Returns:
        ``True`` if the user confirms, ``False`` otherwise.
    """
    print("\n  Files to push:\n")
    for f in files:
        try:
            rel = f.relative_to(output_root.resolve())
        except ValueError:
            rel = f
        classified = _classify_path(f, output_root)
        tag = classified[0] if classified else "unknown"
        print(f"    [{tag}] {rel}")

    print()
    try:
        answer = input("  Proceed with push? [y/N] ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print()
        return False
    return answer in ("y", "yes")


# ---------------------------------------------------------------------------
# Public orchestrators
# ---------------------------------------------------------------------------


def push_manual(
    config: SyncConfig,
    config_path: Path,
    paths: list[str],
    *,
    dry_run: bool = False,
    yes: bool = False,
    data_spec: DataSpec | None = None,
) -> None:
    """Push explicitly listed files and/or a data table replacement.

    Args:
        config: Parsed sync configuration.
        config_path: Path to the ``bq_sync.toml`` that was loaded.
        paths: List of file paths to push.
        dry_run: If ``True``, preview without writing.
        yes: If ``True``, skip interactive confirmation.
        data_spec: Optional table-replace specification.
    """
    output_root = resolve_output_dir(config, config_path)
    project = config.project.id
    region = config.project.default_region

    files = [Path(p).resolve() for p in paths]
    pushable = _filter_pushable(files, output_root)

    unpushable = set(files) - set(pushable)
    for f in unpushable:
        logger.warning("Cannot classify, will skip: %s", f)

    all_items: list[str] = []
    for f in pushable:
        try:
            rel = f.relative_to(output_root.resolve())
        except ValueError:
            rel = f
        all_items.append(str(rel))

    if data_spec:
        all_items.append(
            f"[table-replace] {data_spec.source} -> "
            f"{data_spec.project}/{data_spec.dataset}/{data_spec.table}"
        )

    if not all_items:
        logger.info("Nothing to push.")
        return

    # Always show what will be pushed.
    print("\n  Files to push:\n")
    for item in all_items:
        print(f"    {item}")
    print()

    if dry_run:
        logger.info("Dry-run: no changes written.")
        return

    if not yes:
        try:
            answer = input("  Proceed with push? [y/N] ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print()
            return
        if answer not in ("y", "yes"):
            logger.info("Push cancelled.")
            return

    for f in pushable:
        _push_file(f, output_root, project, region)

    if data_spec:
        bq_client.load_table_from_file(
            data_spec.project,
            data_spec.dataset,
            data_spec.table,
            data_spec.source,
            fmt=data_spec.fmt,
        )

    logger.info("Push complete.")


def push_auto(
    config: SyncConfig,
    config_path: Path,
    *,
    dry_run: bool = False,
    yes: bool = False,
    since_hours: float = 24.0,
) -> None:
    """Detect changed files and push them after confirmation.

    Detection strategy:

    1. **Git** (preferred): ``git status --porcelain`` on the output root.
    2. **Fallback**: files with ``mtime`` within *since_hours*.

    Args:
        config: Parsed sync configuration.
        config_path: Path to the ``bq_sync.toml`` that was loaded.
        dry_run: If ``True``, preview without writing.
        yes: If ``True``, skip interactive confirmation.
        since_hours: Look-back window for the mtime fallback (hours).
    """
    output_root = resolve_output_dir(config, config_path)
    project = config.project.id
    region = config.project.default_region

    if not output_root.exists():
        logger.error("Output root does not exist: %s", output_root)
        sys.exit(1)

    # 1. Try git.
    git_files = _git_changed_files(output_root)
    if git_files is not None:
        files = _filter_pushable(git_files, output_root)
        source_label = "git status"
    else:
        # 2. Fallback to mtime.
        logger.info(
            "Git not available, falling back to mtime (last %.1f hours).",
            since_hours,
        )
        mtime_files = _mtime_changed_files(output_root, since_hours)
        files = _filter_pushable(mtime_files, output_root)
        source_label = f"mtime (last {since_hours}h)"

    if not files:
        logger.info("No changed files detected via %s.", source_label)
        return

    logger.info("Detected %d changed file(s) via %s.", len(files), source_label)

    if dry_run:
        print("\n  Files that would be pushed:\n")
        for f in files:
            try:
                rel = f.relative_to(output_root.resolve())
            except ValueError:
                rel = f
            classified = _classify_path(f, output_root)
            tag = classified[0] if classified else "unknown"
            print(f"    [{tag}] {rel}")
        print()
        logger.info("Dry-run: no changes written.")
        return

    if not yes:
        if not _confirm_changeset(files, output_root):
            logger.info("Push cancelled.")
            return

    for f in files:
        _push_file(f, output_root, project, region)

    logger.info("Push complete.")
    logger.info("Recommendation: commit the pushed changes with git.")


# ---------------------------------------------------------------------------
# RM mode — delete BQ resource, then local file
# ---------------------------------------------------------------------------


def _rm_file(
    file: Path,
    output_root: Path,
    project: str,
    region: str,
) -> None:
    """Delete a single BQ resource identified by its local file path.

    Args:
        file: Absolute path to the local resource file.
        output_root: Project-scoped output directory.
        project: GCP project ID.
        region: GCP region.
    """
    classified = _classify_path(file, output_root)
    if classified is None:
        logger.warning("Cannot classify file, skipping: %s", file)
        return

    resource_type, dataset, name = classified

    if resource_type == _VIEW_DIR and file.suffix == ".sql":
        bq_client.delete_view(project, dataset, name)

    elif resource_type == _VIEW_DIR and file.suffix == ".yaml":
        # View model YAML — delete the view itself.
        update = read_view_model_yaml(file)
        bq_client.delete_view(project, dataset, update.name)

    elif resource_type == _MODEL_DIR and file.suffix == ".yaml":
        update = read_model_yaml(file)
        bq_client.delete_table(project, dataset, update.name)

    elif resource_type == _ROUTINE_DIR and file.suffix == ".sql":
        update = read_routine_sql(file)
        bq_client.delete_routine(project, dataset, update.name)

    elif resource_type == _SAVED_QUERY_DIR and file.suffix == ".sql":
        update = read_saved_query_sql(file)
        bq_client.delete_saved_query(project, region, update.name)

    else:
        logger.warning("Unsupported resource type/extension for rm, skipping: %s", file)
        return

    # BQ delete succeeded — now remove local file.
    file.unlink()
    logger.info("Deleted local file %s", file)


def rm_resources(
    config: SyncConfig,
    config_path: Path,
    paths: list[str],
    *,
    dry_run: bool = False,
    yes: bool = False,
) -> None:
    """Delete BQ resources identified by local file paths.

    First deletes the resource from BigQuery, then deletes the local
    file on success.

    Args:
        config: Parsed sync configuration.
        config_path: Path to the ``bq_sync.toml`` that was loaded.
        paths: List of local file paths to remove.
        dry_run: If ``True``, preview without deleting.
        yes: If ``True``, skip interactive confirmation.
    """
    output_root = resolve_output_dir(config, config_path)
    project = config.project.id
    region = config.project.default_region

    files = [Path(p).resolve() for p in paths]
    removable = _filter_pushable(files, output_root)

    unrecognised = set(files) - set(removable)
    for f in unrecognised:
        logger.warning("Cannot classify, will skip: %s", f)

    if not removable:
        logger.info("Nothing to remove.")
        return

    print("\n  Resources to delete (BQ + local):\n")
    for f in removable:
        try:
            rel = f.relative_to(output_root.resolve())
        except ValueError:
            rel = f
        classified = _classify_path(f, output_root)
        tag = classified[0] if classified else "unknown"
        print(f"    [{tag}] {rel}")
    print()

    if dry_run:
        logger.info("Dry-run: no resources deleted.")
        return

    if not yes:
        try:
            answer = input("  Proceed with deletion? [y/N] ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print()
            return
        if answer not in ("y", "yes"):
            logger.info("Deletion cancelled.")
            return

    for f in removable:
        _rm_file(f, output_root, project, region)

    logger.info("Deletion complete.")
