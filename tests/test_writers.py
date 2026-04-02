"""Tests for ``bq_sync.writers`` and ``bq_sync.humanize``."""

from __future__ import annotations

from datetime import datetime, timezone
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

TS = datetime(2025, 1, 1, tzinfo=timezone.utc)
TS_CREATED = datetime(2024, 6, 15, 12, 0, tzinfo=timezone.utc)


class TestHumanizeBytes:
    """Tests for ``humanize_bytes``."""

    def test_none_returns_empty(self) -> None:
        """None input returns empty string."""
        assert humanize_bytes(None) == ""

    def test_zero(self) -> None:
        """Zero bytes."""
        assert humanize_bytes(0) == "0 B"

    def test_bytes(self) -> None:
        """Small value under 1 KiB."""
        assert humanize_bytes(512) == "512 B"

    def test_kibibytes(self) -> None:
        """Value in KiB range."""
        assert humanize_bytes(1024) == "1.0 KiB"

    def test_mebibytes(self) -> None:
        """Value in MiB range."""
        assert humanize_bytes(1024 * 1024) == "1.0 MiB"

    def test_gibibytes(self) -> None:
        """Value in GiB range."""
        assert humanize_bytes(2 * 1024**3) == "2.0 GiB"

    def test_negative(self) -> None:
        """Negative value."""
        assert humanize_bytes(-1) == "-1 B"


class TestWriteViewSql:
    """Tests for ``write_view_sql``."""

    def test_writes_sql(self, tmp_path: Path) -> None:
        """View SQL is written verbatim."""
        view = ViewInfo(name="my_view", sql="SELECT 1", modified=TS)
        path = tmp_path / "views" / "my_view.sql"
        write_view_sql(path, view)

        assert path.read_text() == "SELECT 1"


class TestWriteViewModelYaml:
    """Tests for ``write_view_model_yaml``."""

    def test_yaml_content(self, tmp_path: Path) -> None:
        """View model YAML contains type, schema, description, timestamps."""
        view = ViewInfo(
            name="active_users",
            sql="SELECT id FROM users",
            modified=TS,
            schema=[
                {
                    "name": "id",
                    "type": "INTEGER",
                    "mode": "REQUIRED",
                    "description": "User ID",
                },
            ],
            description="Active users view",
            created=TS_CREATED,
            region="us-east1",
        )
        path = tmp_path / "models" / "active_users.yaml"
        write_view_model_yaml(path, view)

        content = path.read_text()
        assert "name: active_users" in content
        assert "type: VIEW" in content
        assert '"Active users view"' in content
        assert "created: 2024-06-15" in content
        assert "modified: 2025-01-01" in content
        assert "region: us-east1" in content
        assert "schema:" in content
        assert 'description: "User ID"' in content


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
        """Model YAML contains enriched metadata and field descriptions."""
        table = TableInfo(
            name="events",
            schema=[
                {
                    "name": "id",
                    "type": "INTEGER",
                    "mode": "REQUIRED",
                    "description": "Event ID",
                },
                {
                    "name": "ts",
                    "type": "TIMESTAMP",
                    "mode": "NULLABLE",
                    "description": "",
                },
            ],
            description="Event log",
            row_count=1000,
            modified=TS,
            partitioning="ts",
            clustering=["id"],
            created=TS_CREATED,
            region="US",
            primary_keys=["id"],
            total_logical_bytes=5 * 1024**3,
        )
        path = tmp_path / "models" / "events.yaml"
        write_model_yaml(path, table)

        content = path.read_text()
        assert "name: events" in content
        assert "partitioning: ts" in content
        assert "clustering: [id]" in content
        assert "schema:" in content
        assert "created: 2024-06-15" in content
        assert "modified: 2025-01-01" in content
        assert "region: US" in content
        assert "primary_keys: [id]" in content
        assert "total_logical_bytes: 5.0 GiB" in content
        assert 'description: "Event ID"' in content
        # Empty description shows placeholder.
        assert 'description: ""' in content
        assert content.count("description:") == 3  # table-level + 2 fields

    def test_non_ascii_description_written_as_utf8(self, tmp_path: Path) -> None:
        """Non-ASCII descriptions are written as raw UTF-8."""
        table = TableInfo(
            name="declaraciones",
            schema=[
                {
                    "name": "tipo",
                    "type": "STRING",
                    "mode": "NULLABLE",
                    "description": "Tipo de declaración",
                },
            ],
            description="Información de declaración",
            row_count=0,
            modified=TS,
        )
        path = tmp_path / "models" / "declaraciones.yaml"
        write_model_yaml(path, table)

        content = path.read_text(encoding="utf-8")
        assert "declaración" in content
        assert "\\u00f3" not in content
        assert "\\u00e9" not in content


class TestWriteExternalDefinition:
    """Tests for ``write_external_definition``."""

    def test_yaml_content(self, tmp_path: Path) -> None:
        """External definition YAML has enriched metadata and schema."""
        ext = ExternalTableInfo(
            name="ext_table",
            source_uris=["gs://bucket/file.csv"],
            schema=[
                {
                    "name": "col",
                    "type": "STRING",
                    "mode": "NULLABLE",
                    "description": "A column",
                },
            ],
            source_format="CSV",
            modified=TS,
            description="External feed",
            created=TS_CREATED,
            region="EU",
            total_logical_bytes=2048,
            row_count=50,
        )
        path = tmp_path / "externals" / "ext_table.yaml"
        write_external_definition(path, ext)

        content = path.read_text()
        assert "name: ext_table" in content
        assert "source_format: CSV" in content
        assert "gs://bucket/file.csv" in content
        assert "schema:" in content
        assert '"External feed"' in content
        assert "created: 2024-06-15" in content
        assert "modified: 2025-01-01" in content
        assert "region: EU" in content
        assert "row_count: 50" in content
        assert "total_logical_bytes: 2.0 KiB" in content
        assert 'description: "A column"' in content


class TestWriteRoutineModelYaml:
    """Tests for ``write_routine_model_yaml``."""

    def test_yaml_content(self, tmp_path: Path) -> None:
        """Routine model YAML contains language, timestamps, args, return type."""
        routine = RoutineInfo(
            name="add_numbers",
            sql="RETURN x + y;",
            language="SQL",
            modified=TS,
            description="Adds two numbers",
            created=TS_CREATED,
            arguments=[
                {"name": "x", "type": "INT64", "mode": "IN"},
                {"name": "y", "type": "INT64", "mode": "IN"},
            ],
            return_type="INT64",
        )
        path = tmp_path / "models" / "add_numbers.yaml"
        write_routine_model_yaml(path, routine)

        content = path.read_text()
        assert "name: add_numbers" in content
        assert '"Adds two numbers"' in content
        assert "language: SQL" in content
        assert "created: 2024-06-15" in content
        assert "modified: 2025-01-01" in content
        assert "return_type: INT64" in content
        assert "arguments:" in content
        assert "name: x" in content
        assert "name: y" in content
        assert "mode: IN" in content


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


class TestDescriptionPreservation:
    """Tests that re-writing YAML preserves locally-edited descriptions."""

    def test_model_preserves_model_description(self, tmp_path: Path) -> None:
        """Existing non-empty model description is kept on rewrite."""
        path = tmp_path / "models" / "events.yaml"
        path.parent.mkdir(parents=True)
        # Simulate a locally-edited file.
        path.write_text(
            'name: events\ndescription: "Local doc"\nschema:\n',
            encoding="utf-8",
        )
        table = TableInfo(
            name="events",
            schema=[],
            description="BQ doc",
            row_count=0,
            modified=TS,
        )
        write_model_yaml(path, table)

        content = path.read_text()
        assert '"Local doc"' in content
        assert '"BQ doc"' not in content

    def test_model_uses_bq_when_local_empty(self, tmp_path: Path) -> None:
        """Empty local description falls back to BQ value."""
        path = tmp_path / "models" / "events.yaml"
        path.parent.mkdir(parents=True)
        path.write_text(
            'name: events\ndescription: ""\nschema:\n',
            encoding="utf-8",
        )
        table = TableInfo(
            name="events",
            schema=[],
            description="BQ doc",
            row_count=0,
            modified=TS,
        )
        write_model_yaml(path, table)

        content = path.read_text()
        assert '"BQ doc"' in content

    def test_model_preserves_field_description(self, tmp_path: Path) -> None:
        """Existing non-empty field descriptions are kept on rewrite."""
        path = tmp_path / "models" / "events.yaml"
        path.parent.mkdir(parents=True)
        path.write_text(
            'name: events\ndescription: ""\nschema:\n'
            "  - name: id  type: INTEGER  mode: REQUIRED"
            '  description: "Local field doc"\n',
            encoding="utf-8",
        )
        table = TableInfo(
            name="events",
            schema=[
                {
                    "name": "id",
                    "type": "INTEGER",
                    "mode": "REQUIRED",
                    "description": "BQ field doc",
                },
            ],
            description="",
            row_count=0,
            modified=TS,
        )
        write_model_yaml(path, table)

        content = path.read_text()
        assert '"Local field doc"' in content
        assert '"BQ field doc"' not in content

    def test_view_preserves_description(self, tmp_path: Path) -> None:
        """View model YAML preserves local description."""
        path = tmp_path / "models" / "v.yaml"
        path.parent.mkdir(parents=True)
        path.write_text(
            'name: v\ndescription: "Local view"\ntype: VIEW\nschema:\n',
            encoding="utf-8",
        )
        view = ViewInfo(
            name="v",
            sql="SELECT 1",
            modified=TS,
            description="BQ view",
        )
        write_view_model_yaml(path, view)

        content = path.read_text()
        assert '"Local view"' in content
        assert '"BQ view"' not in content

    def test_routine_preserves_description(self, tmp_path: Path) -> None:
        """Routine model YAML preserves local description."""
        path = tmp_path / "models" / "fn.yaml"
        path.parent.mkdir(parents=True)
        path.write_text(
            'name: fn\ndescription: "Local routine"\nlanguage: SQL\n',
            encoding="utf-8",
        )
        routine = RoutineInfo(
            name="fn",
            sql="RETURN 1;",
            language="SQL",
            modified=TS,
            description="BQ routine",
        )
        write_routine_model_yaml(path, routine)

        content = path.read_text()
        assert '"Local routine"' in content
        assert '"BQ routine"' not in content

    def test_external_preserves_description(self, tmp_path: Path) -> None:
        """External table YAML preserves local description."""
        path = tmp_path / "models" / "ext.yaml"
        path.parent.mkdir(parents=True)
        path.write_text(
            'name: ext\ndescription: "Local ext"\nschema:\n',
            encoding="utf-8",
        )
        ext = ExternalTableInfo(
            name="ext",
            source_uris=["gs://b/f.csv"],
            schema=[],
            source_format="CSV",
            modified=TS,
            description="BQ ext",
        )
        write_external_definition(path, ext)

        content = path.read_text()
        assert '"Local ext"' in content
        assert '"BQ ext"' not in content
