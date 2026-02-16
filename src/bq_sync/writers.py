"""Write BQ resources to the local filesystem (pull mode)."""

from __future__ import annotations

import json
from pathlib import Path

from bq_sync.humanize import humanize_bytes
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


def _format_schema_lines(schema: list[dict[str, str]]) -> list[str]:
    """Build indented YAML lines for a schema list.

    Each field is rendered as a single line with ``name``, ``type``,
    ``mode``, and ``description``.

    Args:
        schema: List of field dicts.

    Returns:
        YAML lines (including leading ``schema:`` header).
    """
    lines = ["schema:"]
    for field in schema:
        desc = field.get("description", "")
        entry = (
            f"  - name: {field['name']}  type: {field['type']}"
            f"  mode: {field['mode']}  description: {json.dumps(desc)}"
        )
        lines.append(entry)
    return lines


def write_model_yaml(path: Path, table: TableInfo) -> None:
    """Write table metadata as a YAML file.

    Uses a minimal YAML serialisation (no external dependency) for
    schema, description, partitioning, clustering, and additional
    metadata such as timestamps, region, primary keys, and logical
    byte size.

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
    if table.created:
        lines.append(f"created: {table.created.isoformat()}")
    lines.append(f"modified: {table.modified.isoformat()}")
    if table.region:
        lines.append(f"region: {table.region}")
    if table.partitioning:
        lines.append(f"partitioning: {table.partitioning}")
    if table.clustering:
        clustering_str = ", ".join(table.clustering)
        lines.append(f"clustering: [{clustering_str}]")
    if table.primary_keys:
        pk_str = ", ".join(table.primary_keys)
        lines.append(f"primary_keys: [{pk_str}]")
    if table.total_logical_bytes is not None:
        lines.append(
            f"total_logical_bytes: {humanize_bytes(table.total_logical_bytes)}"
        )

    lines.extend(_format_schema_lines(table.schema))

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_view_model_yaml(path: Path, view: ViewInfo) -> None:
    """Write view metadata as a YAML model file.

    Produces a model file analogous to table models, with a ``type: VIEW``
    discriminator.

    Args:
        path: Target ``.yaml`` file path.
        view: View resource to write.
    """
    _ensure_parent(path)
    lines = [
        f"name: {view.name}",
        f"description: {json.dumps(view.description)}",
        "type: VIEW",
    ]
    if view.created:
        lines.append(f"created: {view.created.isoformat()}")
    lines.append(f"modified: {view.modified.isoformat()}")
    if view.region:
        lines.append(f"region: {view.region}")

    lines.extend(_format_schema_lines(view.schema))

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_routine_model_yaml(path: Path, routine: RoutineInfo) -> None:
    """Write routine metadata as a YAML model file.

    Includes language, timestamps, argument signatures, and return type.

    Args:
        path: Target ``.yaml`` file path.
        routine: Routine resource to write.
    """
    _ensure_parent(path)
    lines = [
        f"name: {routine.name}",
        f"description: {json.dumps(routine.description)}",
        f"language: {routine.language}",
    ]
    if routine.created:
        lines.append(f"created: {routine.created.isoformat()}")
    lines.append(f"modified: {routine.modified.isoformat()}")
    if routine.return_type:
        lines.append(f"return_type: {routine.return_type}")
    if routine.arguments:
        lines.append("arguments:")
        for arg in routine.arguments:
            lines.append(
                f"  - name: {arg['name']}  type: {arg['type']}  mode: {arg['mode']}"
            )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_external_definition(path: Path, ext: ExternalTableInfo) -> None:
    """Write an external table definition as YAML.

    Includes source URIs, schema with field descriptions, and enriched
    metadata (timestamps, region, size, etc.).

    Args:
        path: Target ``.yaml`` file path.
        ext: External table resource to write.
    """
    _ensure_parent(path)
    lines = [
        f"name: {ext.name}",
        f"description: {json.dumps(ext.description)}",
        f"source_format: {ext.source_format}",
        "source_uris:",
    ]
    for uri in ext.source_uris:
        lines.append(f"  - {uri}")

    if ext.created:
        lines.append(f"created: {ext.created.isoformat()}")
    lines.append(f"modified: {ext.modified.isoformat()}")
    if ext.region:
        lines.append(f"region: {ext.region}")
    lines.append(f"row_count: {ext.row_count}")
    if ext.partitioning:
        lines.append(f"partitioning: {ext.partitioning}")
    if ext.clustering:
        clustering_str = ", ".join(ext.clustering)
        lines.append(f"clustering: [{clustering_str}]")
    if ext.primary_keys:
        pk_str = ", ".join(ext.primary_keys)
        lines.append(f"primary_keys: [{pk_str}]")
    if ext.total_logical_bytes is not None:
        lines.append(f"total_logical_bytes: {humanize_bytes(ext.total_logical_bytes)}")

    lines.extend(_format_schema_lines(ext.schema))

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
