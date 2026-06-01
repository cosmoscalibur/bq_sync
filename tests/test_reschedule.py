"""Tests for ``bq_sync.reschedule`` and the ``reschedule`` CLI subcommand."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from bq_sync.cli import _build_parser
from bq_sync.config import ProjectConfig, SyncConfig
from bq_sync.reschedule import (
    list_scheduled_queries,
    parse_schedule_from_file,
    reschedule_query,
    resync_all_from_files,
    resync_from_file,
)


def _make_config() -> SyncConfig:
    """Return a minimal ``SyncConfig`` for testing."""
    return SyncConfig(
        project=ProjectConfig(id="my-project", default_region="us-east1"),
        datasets=["my_dataset"],
        output_dir=".",
    )


def _make_transfer_config(
    display_name: str = "daily_report",
    schedule: str = "every 24 hours",
) -> MagicMock:
    """Create a mock ``TransferConfig`` protobuf.

    Args:
        display_name: The display name to assign.
        schedule: The schedule string to assign.

    Returns:
        A ``MagicMock`` mimicking ``TransferConfig`` attributes.
    """
    tc = MagicMock()
    tc.display_name = display_name
    tc.schedule = schedule
    tc.data_source_id = "scheduled_query"
    tc.name = f"projects/my-project/locations/us-east1/transferConfigs/123"
    return tc


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------


class TestRescheduleCLIParsing:
    """Verify ``reschedule`` subcommand argument parsing."""

    def test_list_flag(self) -> None:
        """``--list`` is captured correctly."""
        parser = _build_parser()
        args = parser.parse_args(["reschedule", "--list"])

        assert args.command == "reschedule"
        assert args.list is True
        assert args.display_name is None

    def test_display_name_and_schedule(self) -> None:
        """Positional display_name and ``--schedule`` are captured."""
        parser = _build_parser()
        args = parser.parse_args(
            ["reschedule", "daily_report", "--schedule", "every 12 hours"]
        )

        assert args.command == "reschedule"
        assert args.display_name == "daily_report"
        assert args.schedule == "every 12 hours"

    def test_dry_run_flag(self) -> None:
        """``--dry-run`` is supported."""
        parser = _build_parser()
        args = parser.parse_args(
            [
                "reschedule",
                "daily_report",
                "--schedule",
                "every 6 hours",
                "--dry-run",
            ]
        )

        assert args.dry_run is True

    def test_trigger_flag(self) -> None:
        """``--trigger`` is supported."""
        parser = _build_parser()
        args = parser.parse_args(
            [
                "reschedule",
                "daily_report",
                "--schedule",
                "every 6 hours",
                "--trigger",
            ]
        )

        assert args.trigger is True

    def test_defaults(self) -> None:
        """Default values are correct."""
        parser = _build_parser()
        args = parser.parse_args(
            ["reschedule", "daily_report", "--schedule", "every 1 hours"]
        )

        assert args.dry_run is False
        assert args.trigger is False
        assert args.list is False


# ---------------------------------------------------------------------------
# list_scheduled_queries
# ---------------------------------------------------------------------------


class TestListScheduledQueries:
    """Tests for the ``--list`` operation."""

    def test_list_prints_queries(self, capsys: pytest.CaptureFixture) -> None:
        """Scheduled queries are printed to stdout."""
        config = _make_config()
        tc1 = _make_transfer_config("report_a", "every 24 hours")
        tc2 = _make_transfer_config("report_b", "every 6 hours")

        with patch(
            "bq_sync.reschedule.bq_client.list_transfer_configs",
            return_value=[tc1, tc2],
        ):
            list_scheduled_queries(config)

        out = capsys.readouterr().out
        assert "report_a" in out
        assert "every 24 hours" in out
        assert "report_b" in out
        assert "every 6 hours" in out

    def test_list_empty(self) -> None:
        """Empty list logs info without errors."""
        config = _make_config()

        with patch(
            "bq_sync.reschedule.bq_client.list_transfer_configs",
            return_value=[],
        ):
            # Should not raise.
            list_scheduled_queries(config)


# ---------------------------------------------------------------------------
# reschedule_query — dry-run
# ---------------------------------------------------------------------------


class TestRescheduleQueryDryRun:
    """Dry-run mode must not call update or trigger functions."""

    def test_dry_run_no_update(self) -> None:
        """In dry-run, schedule is not updated and no run is triggered."""
        config = _make_config()
        tc = _make_transfer_config("daily_report", "every 24 hours")

        with patch(
            "bq_sync.reschedule.bq_client.get_transfer_config",
            return_value=tc,
        ) as mock_get, patch(
            "bq_sync.reschedule.bq_client.update_transfer_schedule"
        ) as mock_update, patch(
            "bq_sync.reschedule.bq_client.trigger_transfer_run"
        ) as mock_trigger:
            reschedule_query(
                config,
                "daily_report",
                "every 6 hours",
                dry_run=True,
            )

        mock_get.assert_called_once_with("my-project", "us-east1", "daily_report")
        mock_update.assert_not_called()
        mock_trigger.assert_not_called()


# ---------------------------------------------------------------------------
# reschedule_query — execution
# ---------------------------------------------------------------------------


class TestRescheduleQueryExecution:
    """Verify execution path calls update and optionally trigger."""

    def test_update_called(self) -> None:
        """Schedule update is called with the correct arguments."""
        config = _make_config()
        tc = _make_transfer_config("daily_report", "every 24 hours")

        with patch(
            "bq_sync.reschedule.bq_client.get_transfer_config",
            return_value=tc,
        ), patch(
            "bq_sync.reschedule.bq_client.update_transfer_schedule"
        ) as mock_update, patch(
            "bq_sync.reschedule.bq_client.trigger_transfer_run"
        ) as mock_trigger:
            reschedule_query(
                config,
                "daily_report",
                "every 6 hours",
                dry_run=False,
            )

        mock_update.assert_called_once_with(
            "my-project", "us-east1", "daily_report", "every 6 hours"
        )
        mock_trigger.assert_not_called()

    def test_trigger_after_update(self) -> None:
        """When ``--trigger`` is set, a manual run is started."""
        config = _make_config()
        tc = _make_transfer_config("daily_report", "every 24 hours")

        with patch(
            "bq_sync.reschedule.bq_client.get_transfer_config",
            return_value=tc,
        ), patch(
            "bq_sync.reschedule.bq_client.update_transfer_schedule"
        ) as mock_update, patch(
            "bq_sync.reschedule.bq_client.trigger_transfer_run"
        ) as mock_trigger:
            reschedule_query(
                config,
                "daily_report",
                "every 6 hours",
                dry_run=False,
                trigger=True,
            )

        mock_update.assert_called_once()
        mock_trigger.assert_called_once_with(
            "my-project", "us-east1", "daily_report"
        )


# ---------------------------------------------------------------------------
# reschedule_query — not found
# ---------------------------------------------------------------------------


class TestRescheduleQueryNotFound:
    """Verify error handling when the scheduled query does not exist."""

    def test_not_found_exits(self) -> None:
        """``sys.exit(1)`` is called when the query is not found."""
        config = _make_config()

        with patch(
            "bq_sync.reschedule.bq_client.get_transfer_config",
            return_value=None,
        ), pytest.raises(SystemExit) as exc_info:
            reschedule_query(
                config,
                "nonexistent_query",
                "every 6 hours",
            )

        assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# bq_client DataTransfer functions
# ---------------------------------------------------------------------------


class TestListTransferConfigs:
    """Tests for ``bq_client.list_transfer_configs``."""

    def test_filters_scheduled_queries(self) -> None:
        """Only configs with ``data_source_id == 'scheduled_query'``
        are returned.
        """
        sq = MagicMock()
        sq.data_source_id = "scheduled_query"
        sq.display_name = "my_query"

        other = MagicMock()
        other.data_source_id = "google_cloud_storage"
        other.display_name = "gcs_import"

        mock_client = MagicMock()
        mock_client.list_transfer_configs.return_value = [sq, other]

        with patch(
            "bq_sync.bq_client.datatransfer.DataTransferServiceClient",
            return_value=mock_client,
        ):
            from bq_sync.bq_client import list_transfer_configs

            result = list_transfer_configs("my-project", "us-east1")

        assert len(result) == 1
        assert result[0].display_name == "my_query"


class TestGetTransferConfig:
    """Tests for ``bq_client.get_transfer_config``."""

    def test_found(self) -> None:
        """Returns the matching ``TransferConfig``."""
        tc = _make_transfer_config("target_query")

        with patch(
            "bq_sync.bq_client.list_transfer_configs",
            return_value=[tc],
        ):
            from bq_sync.bq_client import get_transfer_config

            result = get_transfer_config("my-project", "us-east1", "target_query")

        assert result is tc

    def test_not_found(self) -> None:
        """Returns ``None`` when no config matches."""
        with patch(
            "bq_sync.bq_client.list_transfer_configs",
            return_value=[],
        ):
            from bq_sync.bq_client import get_transfer_config

            result = get_transfer_config("my-project", "us-east1", "missing")

        assert result is None


class TestUpdateTransferSchedule:
    """Tests for ``bq_client.update_transfer_schedule``."""

    def test_updates_schedule(self) -> None:
        """The schedule field is patched via ``update_transfer_config``."""
        tc = _make_transfer_config("daily_report", "every 24 hours")
        mock_client = MagicMock()

        with patch(
            "bq_sync.bq_client.get_transfer_config",
            return_value=tc,
        ), patch(
            "bq_sync.bq_client.datatransfer.DataTransferServiceClient",
            return_value=mock_client,
        ):
            from bq_sync.bq_client import update_transfer_schedule

            update_transfer_schedule(
                "my-project", "us-east1", "daily_report", "every 6 hours"
            )

        mock_client.update_transfer_config.assert_called_once()

    def test_not_found_raises(self) -> None:
        """``ValueError`` is raised when config is not found."""
        with patch(
            "bq_sync.bq_client.get_transfer_config",
            return_value=None,
        ):
            from bq_sync.bq_client import update_transfer_schedule

            with pytest.raises(ValueError, match="not found"):
                update_transfer_schedule(
                    "my-project", "us-east1", "missing", "every 1 hours"
                )


class TestTriggerTransferRun:
    """Tests for ``bq_client.trigger_transfer_run``."""

    def test_triggers_manual_run(self) -> None:
        """``start_manual_transfer_runs`` is called."""
        tc = _make_transfer_config("daily_report")
        mock_client = MagicMock()

        with patch(
            "bq_sync.bq_client.get_transfer_config",
            return_value=tc,
        ), patch(
            "bq_sync.bq_client.datatransfer.DataTransferServiceClient",
            return_value=mock_client,
        ):
            from bq_sync.bq_client import trigger_transfer_run

            trigger_transfer_run("my-project", "us-east1", "daily_report")

        mock_client.start_manual_transfer_runs.assert_called_once()

    def test_not_found_raises(self) -> None:
        """``ValueError`` is raised when config is not found."""
        with patch(
            "bq_sync.bq_client.get_transfer_config",
            return_value=None,
        ):
            from bq_sync.bq_client import trigger_transfer_run

            with pytest.raises(ValueError, match="not found"):
                trigger_transfer_run("my-project", "us-east1", "missing")


# ---------------------------------------------------------------------------
# CLI parsing: --from-file flag
# ---------------------------------------------------------------------------


class TestRescheduleCLIFromFileParsing:
    """Verify ``--from-file`` argument parsing for the reschedule subcommand."""

    def test_from_file_flag_without_display_name(self) -> None:
        """``--from-file`` is captured as batch mode (no display_name)."""
        parser = _build_parser()
        args = parser.parse_args(["reschedule", "--from-file"])

        assert args.command == "reschedule"
        assert args.from_file is True
        assert args.display_name is None
        assert args.schedule is None

    def test_from_file_flag_with_display_name(self) -> None:
        """``--from-file`` with a display_name targets a single file."""
        parser = _build_parser()
        args = parser.parse_args(
            ["reschedule", "daily_report", "--from-file"]
        )

        assert args.from_file is True
        assert args.display_name == "daily_report"

    def test_from_file_mutually_exclusive_with_schedule(self) -> None:
        """``--from-file`` and ``--schedule`` cannot be used together."""
        parser = _build_parser()

        with pytest.raises(SystemExit):
            parser.parse_args(
                [
                    "reschedule",
                    "daily_report",
                    "--from-file",
                    "--schedule",
                    "every 6 hours",
                ]
            )

    def test_from_file_default_is_false(self) -> None:
        """``--from-file`` defaults to ``False``."""
        parser = _build_parser()
        args = parser.parse_args(
            ["reschedule", "daily_report", "--schedule", "every 1 hours"]
        )

        assert args.from_file is False


# ---------------------------------------------------------------------------
# parse_schedule_from_file
# ---------------------------------------------------------------------------


class TestParseScheduleFromFile:
    """Tests for ``parse_schedule_from_file``."""

    def test_valid_header(self, tmp_path: Path) -> None:
        """Correctly extracts name and schedule from a well-formed header."""
        sql = (
            "-- Scheduled Query: materialize_active_filings\n"
            "-- Schedule: 1 of month 09:30\n"
            "\n"
            "SELECT * FROM my_table\n"
        )
        f = tmp_path / "materialize_active_filings.sql"
        f.write_text(sql, encoding="utf-8")

        name, schedule = parse_schedule_from_file(f)

        assert name == "materialize_active_filings"
        assert schedule == "1 of month 09:30"

    def test_missing_schedule_line(self, tmp_path: Path) -> None:
        """Raises ``ValueError`` when the Schedule header is missing."""
        sql = (
            "-- Scheduled Query: daily_report\n"
            "\n"
            "SELECT 1\n"
        )
        f = tmp_path / "daily_report.sql"
        f.write_text(sql, encoding="utf-8")

        with pytest.raises(ValueError, match="Missing.*Schedule"):
            parse_schedule_from_file(f)

    def test_missing_scheduled_query_line(self, tmp_path: Path) -> None:
        """Raises ``ValueError`` when the Scheduled Query header is missing."""
        sql = (
            "-- Schedule: every 24 hours\n"
            "\n"
            "SELECT 1\n"
        )
        f = tmp_path / "orphan.sql"
        f.write_text(sql, encoding="utf-8")

        with pytest.raises(ValueError, match="Missing.*Scheduled Query"):
            parse_schedule_from_file(f)


# ---------------------------------------------------------------------------
# resync_from_file
# ---------------------------------------------------------------------------


class TestResyncFromFile:
    """Tests for ``resync_from_file``."""

    def _make_sql_file(
        self, tmp_path: Path, name: str, schedule: str
    ) -> Path:
        """Create a temporary SQL file with a valid header."""
        sql = (
            f"-- Scheduled Query: {name}\n"
            f"-- Schedule: {schedule}\n"
            "\n"
            "SELECT 1\n"
        )
        f = tmp_path / f"{name}.sql"
        f.write_text(sql, encoding="utf-8")
        return f

    def test_schedule_differs_updates(self, tmp_path: Path) -> None:
        """When file schedule differs from BQ, update is called."""
        config = _make_config()
        sql_file = self._make_sql_file(
            tmp_path, "daily_report", "every 6 hours"
        )
        tc = _make_transfer_config("daily_report", "every 24 hours")

        with patch(
            "bq_sync.reschedule.bq_client.get_transfer_config",
            return_value=tc,
        ), patch(
            "bq_sync.reschedule.bq_client.update_transfer_schedule"
        ) as mock_update, patch(
            "bq_sync.reschedule.bq_client.trigger_transfer_run"
        ) as mock_trigger:
            resync_from_file(config, sql_file)

        mock_update.assert_called_once_with(
            "my-project", "us-east1", "daily_report", "every 6 hours"
        )
        mock_trigger.assert_not_called()

    def test_schedule_matches_skips(
        self, tmp_path: Path, capsys: pytest.CaptureFixture
    ) -> None:
        """When file schedule matches BQ, no update is performed."""
        config = _make_config()
        sql_file = self._make_sql_file(
            tmp_path, "daily_report", "every 24 hours"
        )
        tc = _make_transfer_config("daily_report", "every 24 hours")

        with patch(
            "bq_sync.reschedule.bq_client.get_transfer_config",
            return_value=tc,
        ), patch(
            "bq_sync.reschedule.bq_client.update_transfer_schedule"
        ) as mock_update:
            resync_from_file(config, sql_file)

        mock_update.assert_not_called()
        out = capsys.readouterr().out
        assert "already in sync" in out

    def test_query_not_found_exits(self, tmp_path: Path) -> None:
        """Exits with code 1 when the query is not found in BQ."""
        config = _make_config()
        sql_file = self._make_sql_file(
            tmp_path, "nonexistent_query", "every 6 hours"
        )

        with patch(
            "bq_sync.reschedule.bq_client.get_transfer_config",
            return_value=None,
        ), pytest.raises(SystemExit) as exc_info:
            resync_from_file(config, sql_file)

        assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# resync_all_from_files
# ---------------------------------------------------------------------------


class TestResyncAllFromFiles:
    """Tests for ``resync_all_from_files``."""

    def test_processes_multiple_files(self, tmp_path: Path) -> None:
        """All .sql files in the directory are processed."""
        config = _make_config()

        # Create two SQL files with different schedules.
        for name, sched in [
            ("query_a", "every 6 hours"),
            ("query_b", "1 of month 09:30"),
        ]:
            sql = (
                f"-- Scheduled Query: {name}\n"
                f"-- Schedule: {sched}\n\nSELECT 1\n"
            )
            (tmp_path / f"{name}.sql").write_text(sql, encoding="utf-8")

        tc_a = _make_transfer_config("query_a", "every 24 hours")
        tc_b = _make_transfer_config("query_b", "every 24 hours")

        def fake_get(_proj: str, _reg: str, name: str) -> MagicMock | None:
            return {"query_a": tc_a, "query_b": tc_b}.get(name)

        with patch(
            "bq_sync.reschedule.bq_client.get_transfer_config",
            side_effect=fake_get,
        ), patch(
            "bq_sync.reschedule.bq_client.update_transfer_schedule"
        ) as mock_update, patch(
            "bq_sync.reschedule.bq_client.trigger_transfer_run"
        ):
            resync_all_from_files(config, tmp_path, dry_run=False)

        assert mock_update.call_count == 2
