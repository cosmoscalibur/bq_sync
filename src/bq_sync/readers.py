"""Read local files (produced by ``pull``) into push update descriptors.

Each reader returns a lightweight dataclass carrying only the mutable
fields needed by the corresponding BQ write function.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class ViewUpdate:
    """Mutable fields for a BigQuery view update."""

    name: str
    sql: str


@dataclass(frozen=True)
class ModelUpdate:
    """Mutable description fields for a table or view model."""

    name: str
    description: str
    field_descriptions: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class SavedQueryUpdate:
    """Mutable fields for a BigQuery saved query update."""

    name: str
    sql: str


@dataclass(frozen=True)
class RoutineUpdate:
    """Mutable fields for a BigQuery routine update (SQL or JS)."""

    name: str
    body: str
    language: str


# ---------------------------------------------------------------------------
# SQL readers
# ---------------------------------------------------------------------------

# Header patterns emitted by ``write_saved_query_sql`` and ``write_routine_sql``.
_SAVED_QUERY_HEADER_RE = re.compile(r"^--\s*Saved Query:\s*(.+)$")
_ROUTINE_NAME_RE = re.compile(r"^--\s*Routine:\s*(.+)$")
_ROUTINE_LANG_RE = re.compile(r"^--\s*Language:\s*(.+)$")


def read_view_sql(path: Path) -> ViewUpdate:
    """Parse a view ``.sql`` file into a ``ViewUpdate``.

    The file name (stem) is used as the view name.  The entire file
    content is taken as the SQL body (views have no metadata header).

    Args:
        path: Path to the ``.sql`` file produced by ``write_view_sql``.

    Returns:
        A ``ViewUpdate`` with *name* and *sql* populated.
    """
    sql = path.read_text(encoding="utf-8")
    return ViewUpdate(name=path.stem, sql=sql)


def read_saved_query_sql(path: Path) -> SavedQueryUpdate:
    """Parse a saved-query ``.sql`` file into a ``SavedQueryUpdate``.

    Strips the ``-- Saved Query: <name>`` header emitted by
    ``write_saved_query_sql`` and uses it for the name.  If the header
    is absent, falls back to the file stem.

    Args:
        path: Path to the ``.sql`` file produced by
            ``write_saved_query_sql``.

    Returns:
        A ``SavedQueryUpdate`` with *name* and *sql* populated.
    """
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)

    name = path.stem
    body_start = 0

    for idx, line in enumerate(lines):
        m = _SAVED_QUERY_HEADER_RE.match(line.rstrip("\n"))
        if m:
            name = m.group(1).strip()
            # Skip the blank line after the header if present.
            body_start = idx + 1
            if body_start < len(lines) and lines[body_start].strip() == "":
                body_start += 1
            break

    sql = "".join(lines[body_start:])
    return SavedQueryUpdate(name=name, sql=sql)


def read_routine_sql(path: Path) -> RoutineUpdate:
    """Parse a routine ``.sql`` file into a ``RoutineUpdate``.

    Strips the ``-- Routine: <name>`` and ``-- Language: <lang>``
    headers emitted by ``write_routine_sql``.  Supports both SQL and
    JavaScript routines.

    Args:
        path: Path to the ``.sql`` file produced by
            ``write_routine_sql``.

    Returns:
        A ``RoutineUpdate`` with *name*, *body*, and *language*.
    """
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)

    name = path.stem
    language = "SQL"
    body_start = 0

    for idx, line in enumerate(lines):
        stripped = line.rstrip("\n")
        m_name = _ROUTINE_NAME_RE.match(stripped)
        if m_name:
            name = m_name.group(1).strip()
            body_start = idx + 1
            continue
        m_lang = _ROUTINE_LANG_RE.match(stripped)
        if m_lang:
            language = m_lang.group(1).strip()
            body_start = idx + 1
            continue
        # Stop scanning once a non-header, non-blank line appears.
        if stripped.strip():
            break

    # Skip blank lines between header and body.
    while body_start < len(lines) and lines[body_start].strip() == "":
        body_start += 1

    body = "".join(lines[body_start:])
    return RoutineUpdate(name=name, body=body, language=language)


# ---------------------------------------------------------------------------
# YAML readers
# ---------------------------------------------------------------------------


def _parse_yaml_field(line: str, key: str) -> str | None:
    """Extract a value for *key* from a YAML-like ``key: value`` line.

    Returns ``None`` when *key* is not present in *line*.

    Args:
        line: A stripped YAML line.
        key: The key to look for (without trailing colon).

    Returns:
        The raw value string, or ``None``.
    """
    prefix = f"{key}:"
    if not line.startswith(prefix):
        return None
    return line[len(prefix) :].strip()


def _decode_json_str(raw: str) -> str:
    """Decode a JSON-encoded string value, falling back to the raw text.

    Args:
        raw: Value string, possibly JSON-quoted.

    Returns:
        Decoded string.
    """
    if not raw:
        return ""
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        return raw


def read_model_yaml(path: Path) -> ModelUpdate:
    """Parse a model ``.yaml`` file into a ``ModelUpdate``.

    Extracts the ``name``, top-level ``description``, and per-field
    ``description`` values from the schema block.  Uses the same
    line-by-line parsing approach as ``writers._load_existing_descriptions``.

    Args:
        path: Path to the ``.yaml`` file produced by ``write_model_yaml``
            or ``write_external_definition``.

    Returns:
        A ``ModelUpdate`` carrying all description fields.
    """
    text = path.read_text(encoding="utf-8")
    return _parse_model_text(text, fallback_name=path.stem)


def read_view_model_yaml(path: Path) -> ModelUpdate:
    """Parse a view model ``.yaml`` file into a ``ModelUpdate``.

    Functionally identical to ``read_model_yaml`` — both formats share
    the same top-level key layout.

    Args:
        path: Path to the ``.yaml`` file produced by
            ``write_view_model_yaml``.

    Returns:
        A ``ModelUpdate`` carrying all description fields.
    """
    text = path.read_text(encoding="utf-8")
    return _parse_model_text(text, fallback_name=path.stem)


def _parse_model_text(text: str, fallback_name: str) -> ModelUpdate:
    """Shared parser for model YAML text.

    Args:
        text: Full file content.
        fallback_name: Name to use when the ``name:`` key is absent.

    Returns:
        A ``ModelUpdate``.
    """
    name: str = fallback_name
    description: str = ""
    field_descriptions: dict[str, str] = {}

    in_schema = False
    for line in text.splitlines():
        stripped = line.strip()

        if not in_schema:
            val = _parse_yaml_field(stripped, "name")
            if val is not None:
                name = val
                continue

            val = _parse_yaml_field(stripped, "description")
            if val is not None:
                description = _decode_json_str(val)
                continue

            if stripped == "schema:":
                in_schema = True
                continue
        else:
            # End of schema block: top-level key without indentation.
            if not line.startswith(" ") and stripped and not stripped.startswith("-"):
                break

            if "name:" in stripped and stripped.startswith("- name:"):
                parts = stripped.split("  ")
                current_field: str | None = None
                field_desc = ""
                for part in parts:
                    part = part.strip().lstrip("- ")
                    if part.startswith("name:"):
                        current_field = part[len("name:") :].strip()
                    elif part.startswith("description:"):
                        raw = part[len("description:") :].strip()
                        field_desc = _decode_json_str(raw)
                if current_field and field_desc:
                    field_descriptions[current_field] = field_desc

    return ModelUpdate(
        name=name,
        description=description,
        field_descriptions=field_descriptions,
    )
