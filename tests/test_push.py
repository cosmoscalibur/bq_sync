"""Tests for ``bq_sync.push``."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from bq_sync.cli import _build_parser
from bq_sync.config import ProjectConfig, SyncConfig
from bq_sync.push import (
    _classify_path,
    _filter_pushable,
    _mtime_changed_files,
    push_auto,
    push_manual,
    rm_resources,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config() -> SyncConfig:
    """Build a minimal ``SyncConfig`` for testing."""
    return SyncConfig(
        project=ProjectConfig(id="my-project", default_region="us-east1"),
        datasets=["my_dataset"],
        output_dir=".",
    )


def _make_output_tree(tmp_path: Path) -> Path:
    """Build a minimal output directory tree and return the output root."""
    output_root = tmp_path / "my-project"
    (output_root / "my_dataset" / "views").mkdir(parents=True)
    (output_root / "my_dataset" / "models").mkdir(parents=True)
    (output_root / "my_dataset" / "routines").mkdir(parents=True)
    (output_root / "saved_queries").mkdir(parents=True)
    return output_root


# ---------------------------------------------------------------------------
# _classify_path
# ---------------------------------------------------------------------------


class TestClassifyPath:
    """Tests for ``_classify_path``."""

    def test_view_sql(self, tmp_path: Path) -> None:
        """View SQL is classified correctly."""
        output_root = _make_output_tree(tmp_path)
        f = output_root / "my_dataset" / "views" / "active.sql"
        f.write_text("SELECT 1")

        result = _classify_path(f, output_root)

        assert result == ("views", "my_dataset", "active")

    def test_model_yaml(self, tmp_path: Path) -> None:
        """Model YAML is classified correctly."""
        output_root = _make_output_tree(tmp_path)
        f = output_root / "my_dataset" / "models" / "users.yaml"
        f.write_text("name: users")

        result = _classify_path(f, output_root)

        assert result == ("models", "my_dataset", "users")

    def test_saved_query_sql(self, tmp_path: Path) -> None:
        """Project-level saved query SQL is classified correctly."""
        output_root = _make_output_tree(tmp_path)
        f = output_root / "saved_queries" / "my_query.sql"
        f.write_text("SELECT 1")

        result = _classify_path(f, output_root)

        assert result == ("saved_queries", "", "my_query")

    def test_unknown_returns_none(self, tmp_path: Path) -> None:
        """Unknown paths return ``None``."""
        output_root = _make_output_tree(tmp_path)
        f = output_root / "data" / "export.csv"
        f.parent.mkdir(parents=True)
        f.write_text("a,b")

        result = _classify_path(f, output_root)

        assert result is None

    def test_outside_root_returns_none(self, tmp_path: Path) -> None:
        """File outside output root returns ``None``."""
        output_root = _make_output_tree(tmp_path)
        f = tmp_path / "outside.sql"
        f.write_text("SELECT 1")

        result = _classify_path(f, output_root)

        assert result is None

    def test_routine_sql(self, tmp_path: Path) -> None:
        """Routine SQL is classified correctly."""
        output_root = _make_output_tree(tmp_path)
        f = output_root / "my_dataset" / "routines" / "my_func.sql"
        f.write_text("RETURN 1")

        result = _classify_path(f, output_root)

        assert result == ("routines", "my_dataset", "my_func")


# ---------------------------------------------------------------------------
# _filter_pushable
# ---------------------------------------------------------------------------


class TestFilterPushable:
    """Tests for ``_filter_pushable``."""

    def test_filters_known_dirs_only(self, tmp_path: Path) -> None:
        """Only files in known resource directories pass the filter."""
        output_root = _make_output_tree(tmp_path)
        view = output_root / "my_dataset" / "views" / "v.sql"
        view.write_text("SELECT 1")
        data = output_root / "data" / "d.csv"
        data.parent.mkdir(parents=True)
        data.write_text("a,b")

        result = _filter_pushable([view, data], output_root)

        assert result == [view]


# ---------------------------------------------------------------------------
# _mtime_changed_files
# ---------------------------------------------------------------------------


class TestMtimeChangedFiles:
    """Tests for ``_mtime_changed_files``."""

    def test_recent_files_detected(self, tmp_path: Path) -> None:
        """Files just created are detected with a large window."""
        output_root = _make_output_tree(tmp_path)
        f = output_root / "my_dataset" / "views" / "v.sql"
        f.write_text("SELECT 1")

        result = _mtime_changed_files(output_root, since_hours=1.0)

        assert f.resolve() in result

    def test_old_files_ignored(self, tmp_path: Path) -> None:
        """A zero-hour window returns no files."""
        output_root = _make_output_tree(tmp_path)
        f = output_root / "my_dataset" / "views" / "v.sql"
        f.write_text("SELECT 1")

        result = _mtime_changed_files(output_root, since_hours=0.0)

        assert f.resolve() not in result


# ---------------------------------------------------------------------------
# push_manual dry-run
# ---------------------------------------------------------------------------


class TestPushManualDryRun:
    """Tests for ``push_manual`` in dry-run mode."""

    def test_dry_run_no_bq_calls(self, tmp_path: Path) -> None:
        """Dry-run mode does not call any BQ write functions."""
        config = _make_config()
        config_path = tmp_path / "bq_sync.toml"
        config_path.write_text("[project]\n[sync]\n")

        output_root = _make_output_tree(tmp_path)
        view = output_root / "my_dataset" / "views" / "active.sql"
        view.write_text("SELECT 1")

        with (
            patch("bq_sync.push.resolve_output_dir", return_value=output_root),
            patch("bq_sync.push.bq_client") as mock_bq,
        ):
            push_manual(
                config,
                config_path,
                paths=[str(view)],
                dry_run=True,
            )

        mock_bq.update_view.assert_not_called()
        mock_bq.update_table_description.assert_not_called()
        mock_bq.update_saved_query.assert_not_called()
        mock_bq.load_table_from_file.assert_not_called()


# ---------------------------------------------------------------------------
# push_auto dry-run
# ---------------------------------------------------------------------------


class TestPushAutoDryRun:
    """Tests for ``push_auto`` in dry-run mode."""

    def test_auto_dry_run_with_git(self, tmp_path: Path) -> None:
        """Auto dry-run via git status lists files but makes no writes."""
        config = _make_config()
        config_path = tmp_path / "bq_sync.toml"
        config_path.write_text("[project]\n[sync]\n")

        output_root = _make_output_tree(tmp_path)
        view = output_root / "my_dataset" / "views" / "v.sql"
        view.write_text("SELECT 1")

        with (
            patch("bq_sync.push.resolve_output_dir", return_value=output_root),
            patch("bq_sync.push._git_changed_files", return_value=[view]),
            patch("bq_sync.push.bq_client") as mock_bq,
        ):
            push_auto(config, config_path, dry_run=True)

        mock_bq.update_view.assert_not_called()

    def test_auto_dry_run_mtime_fallback(self, tmp_path: Path) -> None:
        """Auto dry-run falls back to mtime when git is unavailable."""
        config = _make_config()
        config_path = tmp_path / "bq_sync.toml"
        config_path.write_text("[project]\n[sync]\n")

        output_root = _make_output_tree(tmp_path)
        view = output_root / "my_dataset" / "views" / "v.sql"
        view.write_text("SELECT 1")

        with (
            patch("bq_sync.push.resolve_output_dir", return_value=output_root),
            patch("bq_sync.push._git_changed_files", return_value=None),
            patch("bq_sync.push._mtime_changed_files", return_value=[view]),
            patch("bq_sync.push.bq_client") as mock_bq,
        ):
            push_auto(config, config_path, dry_run=True, since_hours=24.0)

        mock_bq.update_view.assert_not_called()


# ---------------------------------------------------------------------------
# CLI parsing
# ---------------------------------------------------------------------------


class TestPushCLIParsing:
    """Verify ``push`` subcommand argument parsing."""

    def test_auto_mode_defaults(self) -> None:
        """Push without arguments enters auto mode."""
        parser = _build_parser()
        args = parser.parse_args(["push"])

        assert args.command == "push"
        assert args.path is None
        assert args.data is None
        assert args.since == 24.0
        assert args.dry_run is False
        assert args.yes is False

    def test_manual_mode_path(self) -> None:
        """``--path`` triggers manual mode."""
        parser = _build_parser()
        args = parser.parse_args(["push", "--path", "view.sql"])

        assert args.path == ["view.sql"]

    def test_manual_mode_multiple_paths(self) -> None:
        """Multiple ``--path`` flags accumulate."""
        parser = _build_parser()
        args = parser.parse_args(["push", "--path", "a.sql", "--path", "b.yaml"])

        assert args.path == ["a.sql", "b.yaml"]

    def test_data_flag(self) -> None:
        """``--data`` accepts source and destination."""
        parser = _build_parser()
        args = parser.parse_args(["push", "--data", "data.csv", "proj/ds/table"])

        assert args.data == ["data.csv", "proj/ds/table"]

    def test_since_flag(self) -> None:
        """``--since`` overrides the default hours."""
        parser = _build_parser()
        args = parser.parse_args(["push", "--since", "48"])

        assert args.since == 48.0

    def test_yes_flag(self) -> None:
        """``-y`` / ``--yes`` enables skip confirmation."""
        parser = _build_parser()
        args = parser.parse_args(["push", "-y"])

        assert args.yes is True

    def test_dry_run_flag(self) -> None:
        """``--dry-run`` enables dry-run mode."""
        parser = _build_parser()
        args = parser.parse_args(["push", "--dry-run"])

        assert args.dry_run is True


# ---------------------------------------------------------------------------
# rm dry-run
# ---------------------------------------------------------------------------


class TestRmResourcesDryRun:
    """Tests for ``rm_resources`` in dry-run mode."""

    def test_dry_run_no_bq_calls(self, tmp_path: Path) -> None:
        """Dry-run mode does not call any BQ delete functions."""
        config = _make_config()
        config_path = tmp_path / "bq_sync.toml"
        config_path.write_text("[project]\n[sync]\n")

        output_root = _make_output_tree(tmp_path)
        view = output_root / "my_dataset" / "views" / "old.sql"
        view.write_text("SELECT 1")

        with (
            patch("bq_sync.push.resolve_output_dir", return_value=output_root),
            patch("bq_sync.push.bq_client") as mock_bq,
        ):
            rm_resources(
                config,
                config_path,
                paths=[str(view)],
                dry_run=True,
            )

        mock_bq.delete_view.assert_not_called()
        mock_bq.delete_table.assert_not_called()
        mock_bq.delete_routine.assert_not_called()
        mock_bq.delete_saved_query.assert_not_called()
        # Local file should NOT be deleted in dry-run.
        assert view.is_file()


# ---------------------------------------------------------------------------
# rm CLI parsing
# ---------------------------------------------------------------------------


class TestRmCLIParsing:
    """Verify ``rm`` subcommand argument parsing."""

    def test_single_path(self) -> None:
        """``rm`` with a single path."""
        parser = _build_parser()
        args = parser.parse_args(["rm", "view.sql"])

        assert args.command == "rm"
        assert args.path == ["view.sql"]
        assert args.dry_run is False
        assert args.yes is False

    def test_multiple_paths(self) -> None:
        """``rm`` with multiple paths."""
        parser = _build_parser()
        args = parser.parse_args(["rm", "a.sql", "b.yaml"])

        assert args.path == ["a.sql", "b.yaml"]

    def test_dry_run_flag(self) -> None:
        """``--dry-run`` is supported."""
        parser = _build_parser()
        args = parser.parse_args(["rm", "view.sql", "--dry-run"])

        assert args.dry_run is True

    def test_yes_flag(self) -> None:
        """``-y`` skips confirmation."""
        parser = _build_parser()
        args = parser.parse_args(["rm", "view.sql", "-y"])

        assert args.yes is True
