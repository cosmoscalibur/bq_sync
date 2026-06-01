"""Reschedule orchestrator — change the schedule of BigQuery scheduled queries.

Provides the core logic for the ``bq-sync reschedule`` subcommand:

- List all scheduled queries (``--list``).
- Update the recurrence schedule of a named scheduled query.
- Optionally trigger an immediate manual run (``--trigger``).
- Preview changes without applying them (``--dry-run``).
"""

from __future__ import annotations

import logging
import re
import sys
from pathlib import Path

from bq_sync import bq_client
from bq_sync.config import SyncConfig

logger = logging.getLogger(__name__)

# Patterns for the two-line SQL file header written by ``writers.py``.
_RE_NAME = re.compile(r"^--\s*Scheduled Query:\s*(.+)$")
_RE_SCHEDULE = re.compile(r"^--\s*Schedule:\s*(.+)$")


def list_scheduled_queries(config: SyncConfig) -> None:
    """Print all scheduled queries for the configured project/region.

    Each entry shows the display name and its current schedule string.

    Args:
        config: Parsed sync configuration providing project ID and region.
    """
    project = config.project.id
    region = config.project.default_region

    configs = bq_client.list_transfer_configs(project, region)

    if not configs:
        logger.info(
            "No scheduled queries found in %s/%s.", project, region
        )
        return

    print(f"\n  Scheduled queries in {project} ({region}):\n")
    for tc in configs:
        print(f"    • {tc.display_name}")
        print(f"      schedule: {tc.schedule or '(none)'}")
    print()


def reschedule_query(
    config: SyncConfig,
    display_name: str,
    schedule: str,
    *,
    dry_run: bool = False,
    trigger: bool = False,
) -> None:
    """Update the schedule of a BigQuery scheduled query.

    Locates the transfer config by *display_name*, applies the new
    *schedule* string, and optionally triggers an immediate manual run.

    Args:
        config: Parsed sync configuration providing project ID and region.
        display_name: Exact ``display_name`` of the scheduled query.
        schedule: New BigQuery schedule string
            (e.g. ``"every 24 hours"``).
        dry_run: If ``True``, preview the change without writing.
        trigger: If ``True``, trigger a manual run after updating.
    """
    project = config.project.id
    region = config.project.default_region

    # Verify the transfer config exists before attempting changes.
    current = bq_client.get_transfer_config(project, region, display_name)
    if current is None:
        logger.error(
            "Scheduled query '%s' not found in %s/%s.",
            display_name,
            project,
            region,
        )
        sys.exit(1)

    print(f"\n  Query   : {display_name}")
    print(f"  Current : {current.schedule or '(none)'}")
    print(f"  New     : {schedule}")

    if dry_run:
        print()
        logger.info("Dry-run: no changes applied.")
        return

    bq_client.update_transfer_schedule(
        project, region, display_name, schedule
    )
    logger.info("Schedule updated for '%s'.", display_name)

    if trigger:
        logger.info("Triggering manual run for '%s' …", display_name)
        bq_client.trigger_transfer_run(project, region, display_name)
        logger.info("Manual run triggered.")


def parse_schedule_from_file(path: Path) -> tuple[str, str]:
    """Extract scheduled query name and schedule from a SQL file header.

    Expects the first two lines to match the format produced by
    ``writers.write_scheduled_query_sql``::

        -- Scheduled Query: <name>
        -- Schedule: <schedule>

    Args:
        path: Path to a ``.sql`` file with the expected header.

    Returns:
        Tuple of ``(name, schedule)`` extracted from the header.

    Raises:
        ValueError: If the required header lines are missing or malformed.
    """
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()

    name: str | None = None
    schedule: str | None = None

    for line in lines[:10]:  # Header is always in the first few lines.
        if name is None:
            m = _RE_NAME.match(line)
            if m:
                name = m.group(1).strip()
        if schedule is None:
            m = _RE_SCHEDULE.match(line)
            if m:
                schedule = m.group(1).strip()
        if name is not None and schedule is not None:
            break

    if name is None:
        msg = f"Missing '-- Scheduled Query: <name>' header in {path}"
        raise ValueError(msg)
    if schedule is None:
        msg = f"Missing '-- Schedule: <schedule>' header in {path}"
        raise ValueError(msg)

    return name, schedule


def resync_from_file(
    config: SyncConfig,
    path: Path,
    *,
    dry_run: bool = False,
    trigger: bool = False,
) -> None:
    """Sync a single scheduled query's schedule from its SQL file.

    Reads the expected schedule from the file header, compares it with
    the live schedule in BigQuery, and updates only when they differ.

    Args:
        config: Parsed sync configuration providing project ID and region.
        path: Path to a ``.sql`` file with the scheduled query header.
        dry_run: If ``True``, preview the change without writing.
        trigger: If ``True``, trigger a manual run after updating.
    """
    name, file_schedule = parse_schedule_from_file(path)
    project = config.project.id
    region = config.project.default_region

    current = bq_client.get_transfer_config(project, region, name)
    if current is None:
        logger.error(
            "Scheduled query '%s' (from %s) not found in %s/%s.",
            name,
            path,
            project,
            region,
        )
        sys.exit(1)

    current_schedule = current.schedule or ""

    if current_schedule == file_schedule:
        print(f"  ✓ {name}: already in sync ({file_schedule})")
        return

    print(f"\n  Query   : {name}")
    print(f"  Current : {current_schedule or '(none)'}")
    print(f"  File    : {file_schedule}")

    if dry_run:
        print()
        logger.info("Dry-run: no changes applied.")
        return

    bq_client.update_transfer_schedule(project, region, name, file_schedule)
    logger.info("Schedule updated for '%s'.", name)

    if trigger:
        logger.info("Triggering manual run for '%s' …", name)
        bq_client.trigger_transfer_run(project, region, name)
        logger.info("Manual run triggered.")


def resync_all_from_files(
    config: SyncConfig,
    scheduled_queries_dir: Path,
    *,
    dry_run: bool = False,
) -> None:
    """Sync all scheduled queries from SQL files in a directory.

    Iterates every ``.sql`` file in *scheduled_queries_dir* and calls
    ``resync_from_file`` for each.

    Args:
        config: Parsed sync configuration providing project ID and region.
        scheduled_queries_dir: Directory containing ``.sql`` files with
            scheduled query headers.
        dry_run: If ``True``, preview changes without writing.
    """
    sql_files = sorted(scheduled_queries_dir.glob("*.sql"))

    if not sql_files:
        logger.info(
            "No .sql files found in %s.", scheduled_queries_dir
        )
        return

    print(
        f"\n  Syncing {len(sql_files)} scheduled query file(s) "
        f"from {scheduled_queries_dir}\n"
    )

    for sql_path in sql_files:
        resync_from_file(config, sql_path, dry_run=dry_run)
