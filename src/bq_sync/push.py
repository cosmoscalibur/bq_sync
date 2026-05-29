"""Push orchestrator — deploy local resources to BigQuery.

Supports two modes:

- **Manual**: push explicitly listed files (positional arguments),
  and optionally replace a table via ``--data``.
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
    read_routine_model_yaml,
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
        bq_client.upsert_view(project, dataset, update.name, update.sql)

    elif resource_type == _VIEW_DIR and file.suffix == ".yaml":
        # View model YAML — push descriptions.
        update = read_view_model_yaml(file)
        bq_client.upsert_table_description(
            project,
            dataset,
            update.name,
            update.description,
            update.field_descriptions or None,
        )

    elif resource_type == _MODEL_DIR and file.suffix == ".yaml":
        update = read_model_yaml(file)
        bq_client.upsert_table_description(
            project,
            dataset,
            update.name,
            update.description,
            update.field_descriptions or None,
        )

    elif resource_type == _ROUTINE_DIR and file.suffix == ".sql":
        update = read_routine_sql(file)
        # On the create-fallback path, arguments and return_type are needed.
        # Attempt to read the companion model YAML when it exists.
        model_yaml = output_root / dataset / _MODEL_DIR / f"{name}.yaml"
        routine_model = (
            read_routine_model_yaml(model_yaml) if model_yaml.is_file() else None
        )
        bq_client.upsert_routine(
            project,
            dataset,
            update.name,
            update.body,
            language=update.language,
            arguments=routine_model.arguments if routine_model else None,
            return_type=routine_model.return_type if routine_model else None,
        )

    elif resource_type == _ROUTINE_DIR and file.suffix == ".yaml":
        update = read_model_yaml(file)
        bq_client.upsert_routine_description(
            project, dataset, update.name, update.description
        )

    elif resource_type == _SAVED_QUERY_DIR and file.suffix == ".sql":
        update = read_saved_query_sql(file)
        bq_client.upsert_saved_query(project, region, update.name, update.sql)

    else:
        logger.warning("Unsupported resource type/extension, skipping: %s", file)


# ---------------------------------------------------------------------------
# Changeset detection
# ---------------------------------------------------------------------------


def _git_changed_files(output_root: Path) -> list[Path] | None:
    """Return uncommitted changed files via ``git diff``.

    Combines unstaged and staged changes.  Returns ``None`` when git
    is not available or the directory is not a git repository.

    Args:
        output_root: Directory to check.

    Returns:
        List of changed file paths, or ``None`` when git is unavailable.
    """
    work_dir = output_root if output_root.is_dir() else output_root.parent
    try:
        # Discover the repo root so paths resolve correctly.
        toplevel = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=True,
            cwd=work_dir,
        )
        repo_root = Path(toplevel.stdout.strip())
        logger.debug("Git repo root: %s", repo_root)

        # Unstaged changes.
        unstaged = subprocess.run(
            ["git", "diff", "--name-only", "--", str(output_root)],
            capture_output=True,
            text=True,
            check=True,
            cwd=work_dir,
        )
        # Staged changes.
        staged = subprocess.run(
            ["git", "diff", "--name-only", "--cached", "--", str(output_root)],
            capture_output=True,
            text=True,
            check=True,
            cwd=work_dir,
        )
    except (subprocess.CalledProcessError, OSError, FileNotFoundError):
        return None

    raw_lines = unstaged.stdout.strip() + "\n" + staged.stdout.strip()
    logger.debug("git diff raw output:\n%s", raw_lines)

    seen: set[Path] = set()
    files: list[Path] = []
    for line in raw_lines.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        p = (repo_root / line).resolve()
        is_file = p.is_file()
        logger.debug("  path=%r  resolved=%s  is_file=%s", line, p, is_file)
        if is_file and p not in seen:
            seen.add(p)
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
    result = []
    for f in files:
        classified = _classify_path(f, output_root)
        logger.debug("  classify %s -> %s", f, classified)
        if classified is not None:
            result.append(f)
    return result


def _is_materialized_model(file: Path, output_root: Path) -> bool:
    """Check if a model YAML has no corresponding SQL source.

    A model YAML is "materialized" (table or external table) when
    no companion ``.sql`` file exists under ``views/`` or
    ``routines/`` for the same dataset and resource name.

    Args:
        file: Absolute path to a model YAML file.
        output_root: Project-scoped output directory.

    Returns:
        ``True`` when the model has no SQL counterpart.
    """
    classified = _classify_path(file, output_root)
    if classified is None:
        return False

    resource_type, dataset, name = classified
    if resource_type != _MODEL_DIR or file.suffix != ".yaml":
        return False

    # Project-level models have no dataset.
    if not dataset:
        return False

    ds_dir = output_root / dataset
    view_sql = ds_dir / _VIEW_DIR / f"{name}.sql"
    routine_sql = ds_dir / _ROUTINE_DIR / f"{name}.sql"
    return not view_sql.is_file() and not routine_sql.is_file()


def _filter_auto_pushable(
    files: list[Path],
    output_root: Path,
    *,
    include_models: bool = False,
) -> list[Path]:
    """Filter files for auto-push, optionally excluding materialized models.

    Args:
        files: Candidate file paths (already classified as pushable).
        output_root: Project-scoped output directory.
        include_models: When ``False``, exclude materialized model
            YAMLs (tables and external tables without SQL sources).

    Returns:
        Filtered list of auto-pushable files.
    """
    if include_models:
        return files

    result = []
    for f in files:
        if _is_materialized_model(f, output_root):
            logger.debug("  skip materialized model %s", f)
            continue
        result.append(f)
    return result


# SQL priority map: lower value = pushed first.
_SUFFIX_PRIORITY = {".sql": 0, ".yaml": 1}


def _push_order(path: Path) -> int:
    """Sort key: SQL files before YAML so structure exists for descriptions.

    Args:
        path: File path to sort.

    Returns:
        Integer priority (lower = earlier).
    """
    return _SUFFIX_PRIORITY.get(path.suffix, 2)


# ---------------------------------------------------------------------------
# Confirmation prompt
# ---------------------------------------------------------------------------


def _display_changeset(
    header: str,
    files: list[Path],
    output_root: Path,
    *,
    extra_items: list[str] | None = None,
) -> None:
    """Print a tagged changeset to stdout.

    Args:
        header: Section header (e.g. ``"Files to push"``).
        files: Files in the changeset.
        output_root: Project-scoped output directory.
        extra_items: Additional free-text items to display.
    """
    print(f"\n  {header}:\n")
    for f in files:
        try:
            rel = f.relative_to(output_root.resolve())
        except ValueError:
            rel = f
        classified = _classify_path(f, output_root)
        tag = classified[0] if classified else "unknown"
        print(f"    [{tag}] {rel}")
    for item in extra_items or []:
        print(f"    {item}")
    print()


def _confirm(prompt: str) -> bool:
    """Prompt the user for ``[y/N]`` confirmation.

    Handles ``EOFError`` and ``KeyboardInterrupt`` gracefully.

    Args:
        prompt: Prompt text shown before ``[y/N]``.

    Returns:
        ``True`` if the user confirms, ``False`` otherwise.
    """
    try:
        answer = input(f"  {prompt} [y/N] ").strip().lower()
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

    extra: list[str] = []
    if data_spec:
        extra.append(
            f"[table-replace] {data_spec.source} -> "
            f"{data_spec.project}/{data_spec.dataset}/{data_spec.table}"
        )

    if not pushable and not extra:
        logger.info("Nothing to push.")
        return

    _display_changeset("Files to push", pushable, output_root, extra_items=extra)

    if dry_run:
        logger.info("Dry-run: no changes written.")
        return

    if not yes:
        if not _confirm("Proceed with push?"):
            logger.info("Push cancelled.")
            return

    for f in sorted(pushable, key=_push_order):
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
    logger.info("Hint: commit pushed changes with 'git commit'.")


def push_auto(
    config: SyncConfig,
    config_path: Path,
    *,
    dry_run: bool = False,
    yes: bool = False,
    since_hours: float = 24.0,
    include_models: bool = False,
    use_mtime: bool = False,
) -> None:
    """Detect changed files and push them after confirmation.

    Detection strategy:

    1. **Git** (preferred): ``git status --porcelain`` on the output root.
    2. **Fallback**: files with ``mtime`` within *since_hours*.

    When *use_mtime* is ``True``, git detection is skipped and mtime
    is used directly.

    Args:
        config: Parsed sync configuration.
        config_path: Path to the ``bq_sync.toml`` that was loaded.
        dry_run: If ``True``, preview without writing.
        yes: If ``True``, skip interactive confirmation.
        since_hours: Look-back window for the mtime fallback (hours).
        include_models: When ``True``, include materialized model
            YAMLs in the changeset.  Defaults to ``False``.
        use_mtime: When ``True``, skip git detection and use
            mtime-based file scanning directly.
    """
    output_root = resolve_output_dir(config, config_path)
    project = config.project.id
    region = config.project.default_region

    if not output_root.exists():
        logger.error("Output root does not exist: %s", output_root)
        sys.exit(1)

    if use_mtime:
        # Explicit mtime mode — skip git entirely.
        mtime_files = _mtime_changed_files(output_root, since_hours)
        files = _filter_pushable(mtime_files, output_root)
        source_label = f"mtime (last {since_hours}h)"
    else:
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

    # Exclude materialized models unless explicitly included.
    files = _filter_auto_pushable(files, output_root, include_models=include_models)

    if not files:
        logger.info("No changed files detected via %s.", source_label)
        return

    logger.info("Detected %d changed file(s) via %s.", len(files), source_label)

    if dry_run:
        _display_changeset("Files that would be pushed", files, output_root)
        logger.info("Dry-run: no changes written.")
        return

    if not yes:
        _display_changeset("Files to push", files, output_root)
        if not _confirm("Proceed with push?"):
            logger.info("Push cancelled.")
            return

    for f in sorted(files, key=_push_order):
        _push_file(f, output_root, project, region)

    logger.info("Push complete.")
    logger.info("Hint: commit pushed changes with 'git commit'.")


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

    _display_changeset("Resources to delete (BQ + local)", removable, output_root)

    if dry_run:
        logger.info("Dry-run: no resources deleted.")
        return

    if not yes:
        if not _confirm("Proceed with deletion?"):
            logger.info("Deletion cancelled.")
            return

    for f in removable:
        _rm_file(f, output_root, project, region)

    logger.info("Deletion complete.")
    logger.info("Hint: commit deletions with 'git add -A && git commit'.")
