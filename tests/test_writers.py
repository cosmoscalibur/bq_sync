"""Tests for ``bq_sync.writers``."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from bq_sync.resources import (
    ExternalTableInfo,
    RoutineInfo,
    SavedQueryInfo,
    ScheduledQueryInfo,
    TableInfo,
    ViewInfo,
)
from bq_sync.writers import (
    write_external_definition,
    write_model_yaml,
    write_routine_sql,
    write_saved_query_sql,
    write_scheduled_query_sql,
    write_view_sql,
)

TS = datetime(2025, 1, 1, tzinfo=timezone.utc)


class TestWriteViewSql:
    """Tests for ``write_view_sql``."""

    def test_writes_sql(self, tmp_path: Path) -> None:
        """View SQL is written verbatim."""
        view = ViewInfo(name="my_view", sql="SELECT 1", modified=TS)
        path = tmp_path / "views" / "my_view.sql"
        write_view_sql(path, view)

        assert path.read_text() == "SELECT 1"


class TestWriteRoutineSql:
    """Tests for ``write_routine_sql``."""

    def test_header_and_body(self, tmp_path: Path) -> None:
        """Routine file has language header."""
        routine = RoutineInfo(name="fn", sql="RETURN 1;", language="SQL", modified=TS)
        path = tmp_path / "routines" / "fn.sql"
        write_routine_sql(path, routine)

        content = path.read_text()
        assert "-- Routine: fn" in content
        assert "-- Language: SQL" in content
        assert "RETURN 1;" in content


class TestWriteScheduledQuerySql:
    """Tests for ``write_scheduled_query_sql``."""

    def test_header_format(self, tmp_path: Path) -> None:
        """Scheduled query file includes name and schedule in header."""
        sq = ScheduledQueryInfo(
            name="daily_load",
            sql="INSERT ...",
            schedule="every 24 hours",
            modified=TS,
        )
        path = tmp_path / "sq" / "daily_load.sql"
        write_scheduled_query_sql(path, sq)

        content = path.read_text()
        assert "-- Scheduled Query: daily_load" in content
        assert "-- Schedule: every 24 hours" in content
        assert "INSERT ..." in content


class TestWriteModelYaml:
    """Tests for ``write_model_yaml``."""

    def test_schema_fields(self, tmp_path: Path) -> None:
        """Model YAML contains schema, description, and partitioning."""
        table = TableInfo(
            name="events",
            schema=[
                {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "ts", "type": "TIMESTAMP", "mode": "NULLABLE"},
            ],
            description="Event log",
            row_count=1000,
            modified=TS,
            partitioning="ts",
            clustering=["id"],
        )
        path = tmp_path / "models" / "events.yaml"
        write_model_yaml(path, table)

        content = path.read_text()
        assert "name: events" in content
        assert "partitioning: ts" in content
        assert "clustering: [id]" in content
        assert "schema:" in content


class TestWriteExternalDefinition:
    """Tests for ``write_external_definition``."""

    def test_yaml_content(self, tmp_path: Path) -> None:
        """External definition YAML has source_uris and schema."""
        ext = ExternalTableInfo(
            name="ext_table",
            source_uris=["gs://bucket/file.csv"],
            schema=[{"name": "col", "type": "STRING", "mode": "NULLABLE"}],
            source_format="CSV",
            modified=TS,
        )
        path = tmp_path / "externals" / "ext_table.yaml"
        write_external_definition(path, ext)

        content = path.read_text()
        assert "name: ext_table" in content
        assert "source_format: CSV" in content
        assert "gs://bucket/file.csv" in content
        assert "schema:" in content


class TestWriteSavedQuerySql:
    """Tests for ``write_saved_query_sql``."""

    def test_header_and_body(self, tmp_path: Path) -> None:
        """Saved query file has name header."""
        saved = SavedQueryInfo(name="q1", sql="SELECT 2", modified=TS)
        path = tmp_path / "saved" / "q1.sql"
        write_saved_query_sql(path, saved)

        content = path.read_text()
        assert "-- Saved Query: q1" in content
        assert "SELECT 2" in content
