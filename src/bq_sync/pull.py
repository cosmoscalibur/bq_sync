"""Pull orchestrator — fetch BQ resources to local filesystem.

All output paths are resolved against the project-scoped output
directory ``<output_dir>/<project_id>/``.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from bq_sync import bq_client
from bq_sync.config import SyncConfig, resolve_output_dir
from bq_sync.fetch import FetchAction, has_uncommitted_changes, resolve
from bq_sync.writers import (
    write_external_definition,
    write_model_yaml,
    write_routine_model_yaml,
    write_routine_sql,
    write_saved_query_sql,
    write_scheduled_query_sql,
    write_view_model_yaml,
    write_view_sql,
)

logger = logging.getLogger(__name__)


def _should_process(file: Path, force_files: list[str] | None) -> bool:
    """Check if a file is in the force-file list when filtering is active."""
    if force_files is None:
        return True
    return str(file) in force_files or file.name in force_files


def pull_dataset(
    config: SyncConfig,
    output_root: Path,
    dataset: str,
    *,
    dry_run: bool = False,
    force: bool = False,
    force_files: list[str] | None = None,
) -> None:
    """Pull all resources for a single dataset.

    Args:
        config: Parsed sync configuration.
        output_root: Project-scoped output directory.
        dataset: BigQuery dataset ID.
        dry_run: If ``True``, log actions without writing files.
        force: Bypass the fetch decision matrix for all files.
        force_files: When set, only force-fetch the listed files.
    """
    project = config.project.id
    ds_dir = output_root / dataset

    # Views
    for view in bq_client.list_views(project, dataset):
        sql_path = ds_dir / "views" / f"{view.name}.sql"
        model_path = ds_dir / "models" / f"{view.name}.yaml"
        is_forced = force and _should_process(sql_path, force_files)
        action = resolve(view.modified, sql_path, force=is_forced)
        if action == FetchAction.SKIP:
            logger.debug("SKIP view %s", view.name)
            continue
        if action == FetchAction.WARN:
            continue
        logger.info("FETCH view %s -> %s", view.name, sql_path)
        if not dry_run:
            write_view_sql(sql_path, view)
            write_view_model_yaml(model_path, view)

    # Routines
    for routine in bq_client.list_routines(project, dataset):
        sql_path = ds_dir / "routines" / f"{routine.name}.sql"
        model_path = ds_dir / "models" / f"{routine.name}.yaml"
        is_forced = force and _should_process(sql_path, force_files)
        action = resolve(routine.modified, sql_path, force=is_forced)
        if action == FetchAction.SKIP:
            logger.debug("SKIP routine %s", routine.name)
            continue
        if action == FetchAction.WARN:
            continue
        logger.info("FETCH routine %s -> %s", routine.name, sql_path)
        if not dry_run:
            write_routine_sql(sql_path, routine)
            write_routine_model_yaml(model_path, routine)

    # Models (table metadata)
    for table in bq_client.list_tables(project, dataset):
        path = ds_dir / "models" / f"{table.name}.yaml"
        is_forced = force and _should_process(path, force_files)
        action = resolve(table.modified, path, force=is_forced)
        if action == FetchAction.SKIP:
            logger.debug("SKIP model %s", table.name)
            continue
        if action == FetchAction.WARN:
            continue
        logger.info("FETCH model %s -> %s", table.name, path)
        if not dry_run:
            write_model_yaml(path, table)

    # External tables
    for ext in bq_client.list_external_tables(project, dataset):
        path = ds_dir / "models" / f"{ext.name}.yaml"
        is_forced = force and _should_process(path, force_files)
        action = resolve(ext.modified, path, force=is_forced)
        if action == FetchAction.SKIP:
            logger.debug("SKIP external %s", ext.name)
            continue
        if action == FetchAction.WARN:
            continue
        logger.info("FETCH external %s -> %s", ext.name, path)
        if not dry_run:
            write_external_definition(path, ext)


def pull_scheduled_queries(
    config: SyncConfig,
    output_root: Path,
    *,
    dry_run: bool = False,
    force: bool = False,
    force_files: list[str] | None = None,
) -> None:
    """Pull project-level scheduled queries.

    Args:
        config: Parsed sync configuration.
        output_root: Project-scoped output directory.
        dry_run: If ``True``, log actions without writing files.
        force: Bypass the fetch decision matrix for all files.
        force_files: When set, only force-fetch the listed files.
    """
    project = config.project.id
    region = config.project.default_region

    for sq in bq_client.list_scheduled_queries(project, region):
        path = output_root / "scheduled_queries" / f"{sq.name}.sql"
        is_forced = force and _should_process(path, force_files)
        action = resolve(sq.modified, path, force=is_forced)
        if action == FetchAction.SKIP:
            logger.debug("SKIP scheduled query %s", sq.name)
            continue
        if action == FetchAction.WARN:
            continue
        logger.info("FETCH scheduled query %s -> %s", sq.name, path)
        if not dry_run:
            write_scheduled_query_sql(path, sq)


def pull_saved_queries(
    config: SyncConfig,
    output_root: Path,
    *,
    dry_run: bool = False,
    force: bool = False,
    force_files: list[str] | None = None,
) -> None:
    """Pull project-level saved queries via Dataform API.

    Args:
        config: Parsed sync configuration.
        output_root: Project-scoped output directory.
        dry_run: If ``True``, log actions without writing files.
        force: Bypass the fetch decision matrix for all files.
        force_files: When set, only force-fetch the listed files.
    """
    project = config.project.id
    region = config.project.default_region

    for saved in bq_client.list_saved_queries(project, region):
        path = output_root / "saved_queries" / f"{saved.name}.sql"
        is_forced = force and _should_process(path, force_files)
        action = resolve(saved.modified, path, force=is_forced)
        if action == FetchAction.SKIP:
            logger.debug("SKIP saved query %s", saved.name)
            continue
        if action == FetchAction.WARN:
            continue
        logger.info("FETCH saved query %s -> %s", saved.name, path)
        if not dry_run:
            write_saved_query_sql(path, saved)


def pull_project(
    config: SyncConfig,
    config_path: Path,
    *,
    dry_run: bool = False,
    force: bool = False,
    force_files: list[str] | None = None,
) -> None:
    """Pull all resources for a project.

    Checks the uncommitted-changes precondition, resolves the output
    root, and calls dataset/scheduled/saved query pull functions.

    Args:
        config: Parsed sync configuration.
        config_path: Path to the ``bq_sync.toml`` that was loaded.
        dry_run: If ``True``, log actions without writing files.
        force: Bypass the fetch decision matrix for all files.
        force_files: When set, only force-fetch the listed files.
    """
    output_root = resolve_output_dir(config, config_path)

    # Precondition: refuse to run if there are uncommitted changes.
    if output_root.exists() and has_uncommitted_changes(output_root):
        logger.error(
            "Uncommitted changes detected in %s. "
            "Commit or stash changes before running bq-sync pull.",
            output_root,
        )
        sys.exit(1)

    logger.info(
        "Pulling project '%s' to %s",
        config.project.id,
        output_root,
    )

    # Dataset-level resources
    for dataset in config.datasets:
        logger.info("Processing dataset '%s'", dataset)
        pull_dataset(
            config,
            output_root,
            dataset,
            dry_run=dry_run,
            force=force,
            force_files=force_files,
        )

    # Project-level resources
    pull_scheduled_queries(
        config,
        output_root,
        dry_run=dry_run,
        force=force,
        force_files=force_files,
    )
    pull_saved_queries(
        config,
        output_root,
        dry_run=dry_run,
        force=force,
        force_files=force_files,
    )

    logger.info("Pull complete.")
