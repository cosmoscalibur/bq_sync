"""Shared dataclasses for BigQuery resource representations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class ViewInfo:
    """Represents a BigQuery view definition."""

    name: str
    sql: str
    modified: datetime


@dataclass(frozen=True)
class RoutineInfo:
    """Represents a BigQuery routine (function/procedure)."""

    name: str
    sql: str
    language: str
    modified: datetime


@dataclass(frozen=True)
class TableInfo:
    """Represents BigQuery table metadata (for model export)."""

    name: str
    schema: list[dict[str, str]]
    description: str
    row_count: int
    modified: datetime
    partitioning: str | None = None
    clustering: list[str] | None = None


@dataclass(frozen=True)
class ExternalTableInfo:
    """Represents a BigQuery external table definition."""

    name: str
    source_uris: list[str]
    schema: list[dict[str, str]]
    source_format: str
    modified: datetime


@dataclass(frozen=True)
class ScheduledQueryInfo:
    """Represents a BigQuery scheduled query definition."""

    name: str
    sql: str
    schedule: str
    modified: datetime


@dataclass(frozen=True)
class SavedQueryInfo:
    """Represents a BigQuery Studio saved query (via Dataform API)."""

    name: str
    sql: str
    modified: datetime
