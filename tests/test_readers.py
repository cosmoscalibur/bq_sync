"""Tests for ``bq_sync.readers``."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from bq_sync.readers import (
    ModelUpdate,
    RoutineUpdate,
    SavedQueryUpdate,
    ScheduledQueryUpdate,
    ViewUpdate,
    read_model_yaml,
    read_routine_sql,
    read_saved_query_sql,
    read_scheduled_query_sql,
    read_view_model_yaml,
    read_view_sql,
)
from bq_sync.resources import (
    RoutineInfo,
    SavedQueryInfo,
    ScheduledQueryInfo,
    TableInfo,
    ViewInfo,
)
from bq_sync.writers import (
    write_model_yaml,
    write_routine_sql,
    write_saved_query_sql,
    write_scheduled_query_sql,
    write_view_model_yaml,
    write_view_sql,
)

TS = datetime(2025, 1, 1, tzinfo=timezone.utc)
TS_CREATED = datetime(2024, 6, 15, 12, 0, tzinfo=timezone.utc)


class TestReadViewSql:
    """Tests for ``read_view_sql``."""

    def test_round_trip(self, tmp_path: Path) -> None:
        """Parse a view SQL file produced by ``write_view_sql``."""
        sql = "SELECT id, name FROM `project.dataset.users`"
        view = ViewInfo(name="active_users", sql=sql, modified=TS)
        path = tmp_path / "active_users.sql"
        write_view_sql(path, view)

        result = read_view_sql(path)

        assert isinstance(result, ViewUpdate)
        assert result.name == "active_users"
        assert result.sql == sql

    def test_plain_file(self, tmp_path: Path) -> None:
        """Parse a plain SQL file with no writer-generated metadata."""
        path = tmp_path / "custom.sql"
        path.write_text("SELECT 1", encoding="utf-8")

        result = read_view_sql(path)

        assert result.name == "custom"
        assert result.sql == "SELECT 1"


class TestReadSavedQuerySql:
    """Tests for ``read_saved_query_sql``."""

    def test_round_trip(self, tmp_path: Path) -> None:
        """Parse a saved-query SQL file produced by ``write_saved_query_sql``."""
        sql = "SELECT * FROM `project.dataset.orders`"
        saved = SavedQueryInfo(name="all_orders", sql=sql, modified=TS)
        path = tmp_path / "all_orders.sql"
        write_saved_query_sql(path, saved)

        result = read_saved_query_sql(path)

        assert isinstance(result, SavedQueryUpdate)
        assert result.name == "all_orders"
        assert result.sql == sql

    def test_fallback_to_stem(self, tmp_path: Path) -> None:
        """When header is missing, name falls back to file stem."""
        path = tmp_path / "my_query.sql"
        path.write_text("SELECT 42", encoding="utf-8")

        result = read_saved_query_sql(path)

        assert result.name == "my_query"
        assert result.sql == "SELECT 42"


class TestReadModelYaml:
    """Tests for ``read_model_yaml``."""

    def test_round_trip(self, tmp_path: Path) -> None:
        """Parse a model YAML file produced by ``write_model_yaml``."""
        table = TableInfo(
            name="users",
            schema=[
                {
                    "name": "id",
                    "type": "INTEGER",
                    "mode": "REQUIRED",
                    "description": "Primary key",
                },
                {
                    "name": "email",
                    "type": "STRING",
                    "mode": "NULLABLE",
                    "description": "",
                },
            ],
            description="All registered users",
            row_count=100,
            modified=TS,
            created=TS_CREATED,
            region="US",
        )
        path = tmp_path / "models" / "users.yaml"
        write_model_yaml(path, table)

        result = read_model_yaml(path)

        assert isinstance(result, ModelUpdate)
        assert result.name == "users"
        assert result.description == "All registered users"
        assert result.field_descriptions == {"id": "Primary key"}

    def test_empty_descriptions(self, tmp_path: Path) -> None:
        """Model with no descriptions yields empty dict."""
        table = TableInfo(
            name="empty",
            schema=[
                {
                    "name": "col",
                    "type": "STRING",
                    "mode": "NULLABLE",
                    "description": "",
                },
            ],
            description="",
            row_count=0,
            modified=TS,
        )
        path = tmp_path / "empty.yaml"
        write_model_yaml(path, table)

        result = read_model_yaml(path)

        assert result.description == ""
        assert result.field_descriptions == {}


class TestReadViewModelYaml:
    """Tests for ``read_view_model_yaml``."""

    def test_round_trip(self, tmp_path: Path) -> None:
        """Parse a view model YAML file produced by ``write_view_model_yaml``."""
        view = ViewInfo(
            name="recent_orders",
            sql="SELECT * FROM orders WHERE date > '2024-01-01'",
            modified=TS,
            schema=[
                {
                    "name": "order_id",
                    "type": "INTEGER",
                    "mode": "REQUIRED",
                    "description": "Order identifier",
                },
            ],
            description="Recent orders view",
            created=TS_CREATED,
            region="US",
        )
        path = tmp_path / "models" / "recent_orders.yaml"
        write_view_model_yaml(path, view)

        result = read_view_model_yaml(path)

        assert isinstance(result, ModelUpdate)
        assert result.name == "recent_orders"
        assert result.description == "Recent orders view"
        assert result.field_descriptions == {"order_id": "Order identifier"}


class TestReadRoutineSql:
    """Tests for ``read_routine_sql``."""

    def test_round_trip_sql(self, tmp_path: Path) -> None:
        """Parse a SQL routine file produced by ``write_routine_sql``."""
        body = "RETURN x + y;"
        routine = RoutineInfo(
            name="add_nums",
            sql=body,
            language="SQL",
            modified=TS,
        )
        path = tmp_path / "add_nums.sql"
        write_routine_sql(path, routine)

        result = read_routine_sql(path)

        assert isinstance(result, RoutineUpdate)
        assert result.name == "add_nums"
        assert result.body == body
        assert result.language == "SQL"

    def test_round_trip_js(self, tmp_path: Path) -> None:
        """Parse a JavaScript routine file produced by ``write_routine_sql``."""
        body = 'return x + "-" + y;'
        routine = RoutineInfo(
            name="concat_js",
            sql=body,
            language="JAVASCRIPT",
            modified=TS,
        )
        path = tmp_path / "concat_js.sql"
        write_routine_sql(path, routine)

        result = read_routine_sql(path)

        assert isinstance(result, RoutineUpdate)
        assert result.name == "concat_js"
        assert result.body == body
        assert result.language == "JAVASCRIPT"

    def test_fallback_to_stem(self, tmp_path: Path) -> None:
        """When header is missing, name falls back to file stem."""
        path = tmp_path / "my_func.sql"
        path.write_text("RETURN 42;", encoding="utf-8")

        result = read_routine_sql(path)

        assert result.name == "my_func"
        assert result.body == "RETURN 42;"
        assert result.language == "SQL"


class TestReadScheduledQuerySql:
    """Tests for ``read_scheduled_query_sql``."""

    def test_round_trip(self, tmp_path: Path) -> None:
        """Parse a scheduled-query SQL file produced by the writer."""
        sql = "CREATE OR REPLACE TABLE `p.d.t` AS SELECT * FROM `p.d.src`"
        sq = ScheduledQueryInfo(
            name="daily_load",
            sql=sql,
            schedule="every 24 hours",
            modified=TS,
        )
        path = tmp_path / "daily_load.sql"
        write_scheduled_query_sql(path, sq)

        result = read_scheduled_query_sql(path)

        assert isinstance(result, ScheduledQueryUpdate)
        assert result.name == "daily_load"
        assert result.sql == sql

    def test_fallback_to_stem(self, tmp_path: Path) -> None:
        """When header is missing, name falls back to file stem."""
        path = tmp_path / "my_scheduled.sql"
        path.write_text("SELECT 1", encoding="utf-8")

        result = read_scheduled_query_sql(path)

        assert result.name == "my_scheduled"
        assert result.sql == "SELECT 1"

    def test_strips_schedule_header(self, tmp_path: Path) -> None:
        """The ``-- Schedule:`` header line is not included in the SQL body."""
        path = tmp_path / "nightly.sql"
        path.write_text(
            "-- Scheduled Query: nightly\n"
            "-- Schedule: every 24 hours\n"
            "\n"
            "SELECT * FROM t\n",
            encoding="utf-8",
        )

        result = read_scheduled_query_sql(path)

        assert result.name == "nightly"
        assert "Schedule" not in result.sql
        assert result.sql == "SELECT * FROM t\n"
