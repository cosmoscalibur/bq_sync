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


def _load_existing_descriptions(path: Path) -> tuple[str | None, dict[str, str]]:
    """Extract descriptions from an existing YAML model file.

    Parses the top-level ``description:`` value and per-field
    ``description:`` values inside the ``schema:`` block using
    line-by-line text matching (no external YAML dependency).

    Args:
        path: Path to an existing YAML file.

    Returns:
        Tuple of (model_description, {field_name: field_description}).
        ``model_description`` is ``None`` when file does not exist or
        the key is absent.  Field descriptions are only collected for
        fields whose ``description`` is non-empty.
    """
    if not path.is_file():
        return None, {}

    text = path.read_text(encoding="utf-8")

    # --- top-level description ---
    model_desc: str | None = None
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("description:"):
            raw = stripped[len("description:") :].strip()
            # Value is JSON-encoded (e.g. "My desc" or "").
            try:
                model_desc = json.loads(raw) if raw else None
            except (json.JSONDecodeError, ValueError):
                model_desc = raw
            break

    # --- per-field descriptions ---
    field_descs: dict[str, str] = {}
    in_schema = False
    current_field: str | None = None
    for line in text.splitlines():
        stripped = line.strip()
        if stripped == "schema:":
            in_schema = True
            continue
        if not in_schema:
            continue
        # End of schema block: a top-level key without leading spaces.
        if not line.startswith(" ") and stripped and not stripped.startswith("-"):
            break

        if "name:" in stripped and stripped.startswith("- name:"):
            # Field entry: "- name: foo  type: …  description: …"
            parts = stripped.split("  ")
            current_field = None
            field_desc_val = ""
            for part in parts:
                part = part.strip().lstrip("- ")
                if part.startswith("name:"):
                    current_field = part[len("name:") :].strip()
                elif part.startswith("description:"):
                    raw = part[len("description:") :].strip()
                    try:
                        field_desc_val = json.loads(raw) if raw else ""
                    except (json.JSONDecodeError, ValueError):
                        field_desc_val = raw
            if current_field and field_desc_val:
                field_descs[current_field] = field_desc_val

    return model_desc, field_descs


def _merge_description(
    bq_desc: str,
    local_desc: str | None,
) -> str:
    """Return the description to write, preferring local over BQ.

    Args:
        bq_desc: Description fetched from BigQuery.
        local_desc: Description found in the existing local file.

    Returns:
        The local description when non-empty, otherwise the BQ value.
    """
    if local_desc:
        return local_desc
    return bq_desc


def _merge_field_descriptions(
    schema: list[dict[str, str]],
    local_fields: dict[str, str],
) -> list[dict[str, str]]:
    """Return schema with locally-edited field descriptions preserved.

    Args:
        schema: Field list from BigQuery.
        local_fields: Mapping of field name → local description.

    Returns:
        New schema list with merged descriptions.
    """
    merged = []
    for field in schema:
        name = field["name"]
        if name in local_fields:
            field = {**field, "description": local_fields[name]}
        merged.append(field)
    return merged


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
        desc_json = json.dumps(desc, ensure_ascii=False)
        entry = (
            f"  - name: {field['name']}  type: {field['type']}"
            f"  mode: {field['mode']}  description: {desc_json}"
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
    local_desc, local_fields = _load_existing_descriptions(path)
    desc = _merge_description(table.description, local_desc)
    schema = _merge_field_descriptions(table.schema, local_fields)
    lines = [
        f"name: {table.name}",
        f"description: {json.dumps(desc, ensure_ascii=False)}",
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

    lines.extend(_format_schema_lines(schema))

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
    local_desc, local_fields = _load_existing_descriptions(path)
    desc = _merge_description(view.description, local_desc)
    schema = _merge_field_descriptions(view.schema, local_fields)
    lines = [
        f"name: {view.name}",
        f"description: {json.dumps(desc, ensure_ascii=False)}",
        "type: VIEW",
    ]
    if view.created:
        lines.append(f"created: {view.created.isoformat()}")
    lines.append(f"modified: {view.modified.isoformat()}")
    if view.region:
        lines.append(f"region: {view.region}")

    lines.extend(_format_schema_lines(schema))

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_routine_model_yaml(path: Path, routine: RoutineInfo) -> None:
    """Write routine metadata as a YAML model file.

    Includes language, timestamps, argument signatures, and return type.

    Args:
        path: Target ``.yaml`` file path.
        routine: Routine resource to write.
    """
    _ensure_parent(path)
    local_desc, _ = _load_existing_descriptions(path)
    desc = _merge_description(routine.description, local_desc)
    lines = [
        f"name: {routine.name}",
        f"description: {json.dumps(desc, ensure_ascii=False)}",
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
    local_desc, local_fields = _load_existing_descriptions(path)
    desc = _merge_description(ext.description, local_desc)
    schema = _merge_field_descriptions(ext.schema, local_fields)
    lines = [
        f"name: {ext.name}",
        f"description: {json.dumps(desc, ensure_ascii=False)}",
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

    lines.extend(_format_schema_lines(schema))

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
