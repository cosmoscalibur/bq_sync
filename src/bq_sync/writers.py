"""Write BQ resources to the local filesystem (pull mode)."""

from __future__ import annotations

import json
from pathlib import Path

from bq_sync.resources import (
    ExternalTableInfo,
    RoutineInfo,
    SavedQueryInfo,
    ScheduledQueryInfo,
    TableInfo,
    ViewInfo,
)


def _ensure_parent(path: Path) -> None:
    """Create parent directories if they do not exist."""
    path.parent.mkdir(parents=True, exist_ok=True)


def write_view_sql(path: Path, view: ViewInfo) -> None:
    """Write a view SQL definition to *path*.

    Args:
        path: Target ``.sql`` file path.
        view: View resource to write.
    """
    _ensure_parent(path)
    path.write_text(view.sql, encoding="utf-8")


def write_routine_sql(path: Path, routine: RoutineInfo) -> None:
    """Write a routine SQL body to *path*.

    Args:
        path: Target ``.sql`` file path.
        routine: Routine resource to write.
    """
    _ensure_parent(path)
    header = f"-- Routine: {routine.name}\n-- Language: {routine.language}\n\n"
    path.write_text(header + routine.sql, encoding="utf-8")


def write_scheduled_query_sql(path: Path, sq: ScheduledQueryInfo) -> None:
    """Write a scheduled query SQL with metadata header.

    Args:
        path: Target ``.sql`` file path.
        sq: Scheduled query resource to write.
    """
    _ensure_parent(path)
    header = f"-- Scheduled Query: {sq.name}\n-- Schedule: {sq.schedule}\n\n"
    path.write_text(header + sq.sql, encoding="utf-8")


def write_model_yaml(path: Path, table: TableInfo) -> None:
    """Write table metadata as a YAML file.

    Uses a minimal YAML serialisation (no external dependency) for
    schema, description, partitioning, and clustering information.

    Args:
        path: Target ``.yaml`` file path.
        table: Table resource to write.
    """
    _ensure_parent(path)
    lines = [
        f"name: {table.name}",
        f"description: {json.dumps(table.description)}",
        f"row_count: {table.row_count}",
    ]
    if table.partitioning:
        lines.append(f"partitioning: {table.partitioning}")
    if table.clustering:
        clustering_str = ", ".join(table.clustering)
        lines.append(f"clustering: [{clustering_str}]")

    lines.append("schema:")
    for field in table.schema:
        lines.append(
            f"  - name: {field['name']}  type: {field['type']}  mode: {field['mode']}"
        )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_external_definition(path: Path, ext: ExternalTableInfo) -> None:
    """Write an external table definition as YAML.

    Args:
        path: Target ``.yaml`` file path.
        ext: External table resource to write.
    """
    _ensure_parent(path)
    lines = [
        f"name: {ext.name}",
        f"source_format: {ext.source_format}",
        "source_uris:",
    ]
    for uri in ext.source_uris:
        lines.append(f"  - {uri}")

    lines.append("schema:")
    for field in ext.schema:
        lines.append(
            f"  - name: {field['name']}  type: {field['type']}  mode: {field['mode']}"
        )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_saved_query_sql(path: Path, saved: SavedQueryInfo) -> None:
    """Write a saved query SQL definition to *path*.

    Args:
        path: Target ``.sql`` file path.
        saved: Saved query resource to write.
    """
    _ensure_parent(path)
    header = f"-- Saved Query: {saved.name}\n\n"
    path.write_text(header + saved.sql, encoding="utf-8")
