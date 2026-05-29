"""Tests for ``bq_sync.push``."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from bq_sync.cli import _build_parser
from bq_sync.config import ProjectConfig, SyncConfig
from bq_sync.push import (
    _classify_path,
    _confirm,
    _display_changeset,
    _filter_auto_pushable,
    _filter_pushable,
    _is_materialized_model,
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

    def test_auto_dry_run_use_mtime(self, tmp_path: Path) -> None:
        """Auto dry-run with use_mtime skips git detection."""
        config = _make_config()
        config_path = tmp_path / "bq_sync.toml"
        config_path.write_text("[project]\n[sync]\n")

        output_root = _make_output_tree(tmp_path)
        view = output_root / "my_dataset" / "views" / "v.sql"
        view.write_text("SELECT 1")

        with (
            patch(
                "bq_sync.push.resolve_output_dir",
                return_value=output_root,
            ),
            patch("bq_sync.push._git_changed_files") as mock_git,
            patch(
                "bq_sync.push._mtime_changed_files",
                return_value=[view],
            ),
            patch("bq_sync.push.bq_client"),
        ):
            push_auto(
                config,
                config_path,
                dry_run=True,
                use_mtime=True,
                since_hours=48.0,
            )

        mock_git.assert_not_called()


# ---------------------------------------------------------------------------
# Materialized model filtering
# ---------------------------------------------------------------------------


class TestIsMaterializedModel:
    """Tests for ``_is_materialized_model``."""

    def test_table_model_without_sql_is_materialized(self, tmp_path: Path) -> None:
        """Model YAML with no views/ or routines/ SQL is materialized."""
        output_root = _make_output_tree(tmp_path)
        model = output_root / "my_dataset" / "models" / "events.yaml"
        model.write_text("name: events")

        assert _is_materialized_model(model, output_root) is True

    def test_view_model_with_sql_is_not_materialized(self, tmp_path: Path) -> None:
        """Model YAML with a companion view SQL is NOT materialized."""
        output_root = _make_output_tree(tmp_path)
        view_sql = output_root / "my_dataset" / "views" / "v.sql"
        view_sql.write_text("SELECT 1")
        model = output_root / "my_dataset" / "models" / "v.yaml"
        model.write_text("name: v")

        assert _is_materialized_model(model, output_root) is False

    def test_routine_model_with_sql_is_not_materialized(self, tmp_path: Path) -> None:
        """Model YAML with a companion routine SQL is NOT materialized."""
        output_root = _make_output_tree(tmp_path)
        routine_sql = output_root / "my_dataset" / "routines" / "fn.sql"
        routine_sql.write_text("RETURN 1;")
        model = output_root / "my_dataset" / "models" / "fn.yaml"
        model.write_text("name: fn")

        assert _is_materialized_model(model, output_root) is False

    def test_non_model_file_is_not_materialized(self, tmp_path: Path) -> None:
        """SQL file in views/ is not a materialized model."""
        output_root = _make_output_tree(tmp_path)
        view_sql = output_root / "my_dataset" / "views" / "v.sql"
        view_sql.write_text("SELECT 1")

        assert _is_materialized_model(view_sql, output_root) is False


class TestFilterAutoPushable:
    """Tests for ``_filter_auto_pushable``."""

    def test_excludes_materialized_by_default(self, tmp_path: Path) -> None:
        """Materialized model YAMLs are excluded by default."""
        output_root = _make_output_tree(tmp_path)
        view_sql = output_root / "my_dataset" / "views" / "v.sql"
        view_sql.write_text("SELECT 1")
        view_model = output_root / "my_dataset" / "models" / "v.yaml"
        view_model.write_text("name: v")
        table_model = output_root / "my_dataset" / "models" / "events.yaml"
        table_model.write_text("name: events")

        files = [view_sql, view_model, table_model]
        result = _filter_auto_pushable(files, output_root)

        assert view_sql in result
        assert view_model in result
        assert table_model not in result

    def test_include_models_keeps_all(self, tmp_path: Path) -> None:
        """With include_models=True, nothing is excluded."""
        output_root = _make_output_tree(tmp_path)
        table_model = output_root / "my_dataset" / "models" / "events.yaml"
        table_model.write_text("name: events")

        files = [table_model]
        result = _filter_auto_pushable(files, output_root, include_models=True)

        assert table_model in result


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
        assert args.paths == []
        assert args.data is None
        assert args.since is None
        assert args.dry_run is False
        assert args.yes is False

    def test_manual_mode_single_path(self) -> None:
        """Single positional path triggers manual mode."""
        parser = _build_parser()
        args = parser.parse_args(["push", "view.sql"])

        assert args.paths == ["view.sql"]

    def test_manual_mode_multiple_paths(self) -> None:
        """Multiple positional paths accumulate."""
        parser = _build_parser()
        args = parser.parse_args(["push", "a.sql", "b.yaml"])

        assert args.paths == ["a.sql", "b.yaml"]

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


class TestIncludeModelsCLI:
    """Verify ``--include-models`` flag parsing."""

    def test_include_models_flag(self) -> None:
        """``--include-models`` enables materialized model push."""
        parser = _build_parser()
        args = parser.parse_args(["push", "--include-models"])

        assert args.include_models is True

    def test_include_models_default_false(self) -> None:
        """``--include-models`` defaults to ``False``."""
        parser = _build_parser()
        args = parser.parse_args(["push"])

        assert args.include_models is False


class TestVersionFlag:
    """Verify ``--version`` flag."""

    def test_version_output(self) -> None:
        """``--version`` prints version and exits."""
        parser = _build_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(["--version"])

        assert exc_info.value.code == 0


class TestDisplayChangeset:
    """Verify ``_display_changeset`` output formatting."""

    def test_tagged_output(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Files are displayed with resource-type tags."""
        output_root = _make_output_tree(tmp_path)
        view = output_root / "my_dataset" / "views" / "active.sql"
        view.write_text("SELECT 1")

        _display_changeset("Files to push", [view], output_root)

        captured = capsys.readouterr()
        assert "[views]" in captured.out
        assert "active.sql" in captured.out
        assert "Files to push:" in captured.out

    def test_extra_items(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Extra items appear after file list."""
        output_root = _make_output_tree(tmp_path)

        _display_changeset(
            "Files to push",
            [],
            output_root,
            extra_items=["[table-replace] data.csv -> proj/ds/tbl"],
        )

        captured = capsys.readouterr()
        assert "[table-replace]" in captured.out


class TestConfirm:
    """Verify ``_confirm`` helper."""

    def test_yes_input(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``y`` input returns ``True``."""
        monkeypatch.setattr("builtins.input", lambda _: "y")

        assert _confirm("Proceed?") is True

    def test_no_input(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``n`` input returns ``False``."""
        monkeypatch.setattr("builtins.input", lambda _: "n")

        assert _confirm("Proceed?") is False

    def test_empty_input(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Empty input (enter) returns ``False`` (default-deny)."""
        monkeypatch.setattr("builtins.input", lambda _: "")

        assert _confirm("Proceed?") is False

    def test_eof_returns_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``EOFError`` (piped stdin) returns ``False``."""

        def raise_eof(_: str) -> str:
            raise EOFError

        monkeypatch.setattr("builtins.input", raise_eof)

        assert _confirm("Proceed?") is False


# ---------------------------------------------------------------------------
# Upsert path verification
# ---------------------------------------------------------------------------


class TestPushFileUsesUpsert:
    """Verify that _push_file dispatches to upsert_* not update_*."""

    def test_view_sql_calls_upsert_view(self, tmp_path: Path) -> None:
        """Pushing a view SQL file calls upsert_view."""
        from bq_sync.push import _push_file

        output_root = _make_output_tree(tmp_path)
        view = output_root / "my_dataset" / "views" / "active.sql"
        view.write_text("SELECT 1")

        with patch("bq_sync.push.bq_client") as mock_bq:
            _push_file(view, output_root, "my-project", "us-east1")

        mock_bq.upsert_view.assert_called_once()
        # update_view must NOT be called directly from _push_file.
        mock_bq.update_view.assert_not_called()

    def test_model_yaml_calls_upsert_table_description(self, tmp_path: Path) -> None:
        """Pushing a model YAML calls upsert_table_description."""
        from bq_sync.push import _push_file

        output_root = _make_output_tree(tmp_path)
        model = output_root / "my_dataset" / "models" / "events.yaml"
        model.write_text('name: events\ndescription: ""\nschema:\n')

        with patch("bq_sync.push.bq_client") as mock_bq:
            _push_file(model, output_root, "my-project", "us-east1")

        mock_bq.upsert_table_description.assert_called_once()
        mock_bq.update_table_description.assert_not_called()

    def test_routine_sql_calls_upsert_routine(self, tmp_path: Path) -> None:
        """Pushing a routine SQL file calls upsert_routine."""
        from bq_sync.push import _push_file

        output_root = _make_output_tree(tmp_path)
        routine = output_root / "my_dataset" / "routines" / "fn.sql"
        routine.write_text(
            "-- Routine: fn\n-- Language: SQL\n\nRETURN 1;\n"
        )

        with patch("bq_sync.push.bq_client") as mock_bq:
            _push_file(routine, output_root, "my-project", "us-east1")

        mock_bq.upsert_routine.assert_called_once()
        mock_bq.update_routine.assert_not_called()

    def test_routine_sql_passes_model_yaml_args(self, tmp_path: Path) -> None:
        """When a companion model YAML exists, its arguments are forwarded."""
        from bq_sync.push import _push_file

        output_root = _make_output_tree(tmp_path)
        routine = output_root / "my_dataset" / "routines" / "fn.sql"
        routine.write_text("-- Routine: fn\n-- Language: SQL\n\nRETURN x;\n")
        # Write a companion model YAML with argument metadata.
        model = output_root / "my_dataset" / "models" / "fn.yaml"
        model.write_text(
            'name: fn\ndescription: ""\nlanguage: SQL\n'
            "return_type: INT64\n"
            "arguments:\n  - name: x  type: INT64  mode: IN\n"
        )

        with patch("bq_sync.push.bq_client") as mock_bq:
            _push_file(routine, output_root, "my-project", "us-east1")

        call_kwargs = mock_bq.upsert_routine.call_args
        # arguments and return_type are passed as keyword args.
        assert call_kwargs.kwargs.get("return_type") == "INT64"
        assert call_kwargs.kwargs.get("arguments") == [
            {"name": "x", "type": "INT64", "mode": "IN"}
        ]
