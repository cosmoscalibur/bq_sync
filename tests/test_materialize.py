"""Tests for ``bq_sync.materialize``."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from bq_sync.cli import _build_parser
from bq_sync.config import ProjectConfig, SyncConfig
from bq_sync.materialize import _build_ddl, _default_target_name, materialize_resource


def _make_config() -> SyncConfig:
    return SyncConfig(
        project=ProjectConfig(id="my-project", default_region="us-east1"),
        datasets=["my_dataset"],
        output_dir=".",
    )


# ---------------------------------------------------------------------------
# _default_target_name
# ---------------------------------------------------------------------------


class TestDefaultTargetName:
    """Tests for prefix-based default target name derivation."""

    def test_view_prefix_replaced(self) -> None:
        """``view_`` prefix becomes ``materialize_``."""
        assert (
            _default_target_name("view_auth_user_valid")
            == "materialize_auth_user_valid"
        )

    def test_external_prefix_replaced(self) -> None:
        """``external_`` prefix becomes ``materialize_``."""
        assert (
            _default_target_name("external_nps_contadia")
            == "materialize_nps_contadia"
        )

    def test_no_known_prefix_unchanged(self) -> None:
        """Names without a recognised prefix are returned unchanged."""
        assert _default_target_name("snapshot_events") == "snapshot_events"

    def test_only_prefix_no_suffix(self) -> None:
        """A name that is exactly the prefix produces ``materialize_``
        with empty suffix.
        """
        assert _default_target_name("view_") == "materialize_"


# ---------------------------------------------------------------------------
# _build_ddl
# ---------------------------------------------------------------------------


class TestBuildDDL:
    """Tests for ``_build_ddl`` DDL string generation."""

    def test_basic_ddl(self) -> None:
        """DDL uses CREATE OR REPLACE TABLE and SELECT * FROM."""
        ddl = _build_ddl("proj", "ds", "view_foo", "materialize_foo")

        assert "CREATE OR REPLACE TABLE" in ddl
        assert "`proj.ds.materialize_foo`" in ddl
        assert "SELECT * FROM `proj.ds.view_foo`" in ddl

    def test_custom_target(self) -> None:
        """Custom target name appears in the DDL."""
        ddl = _build_ddl("proj", "ds", "view_foo", "custom_target")

        assert "`proj.ds.custom_target`" in ddl

    def test_same_source_and_target(self) -> None:
        """Source and target can share the same name."""
        ddl = _build_ddl("proj", "ds", "my_table", "my_table")

        assert ddl.count("`proj.ds.my_table`") == 2


# ---------------------------------------------------------------------------
# materialize_resource — dry-run
# ---------------------------------------------------------------------------


class TestMaterializeResourceDryRun:
    """Dry-run mode must not call run_query or write_model_yaml."""

    def test_dry_run_no_execution(self, tmp_path: Path) -> None:
        """In dry-run, BQ query is not executed and no YAML is written."""
        config = _make_config()
        config_path = tmp_path / "bq_sync.toml"
        config_path.write_text("[project]\n[sync]\n")

        with (
            patch("bq_sync.materialize.resolve_output_dir", return_value=tmp_path),
            patch("bq_sync.materialize.bq_client") as mock_bq,
            patch("bq_sync.materialize.write_model_yaml") as mock_write,
        ):
            materialize_resource(
                config,
                config_path,
                "my-project/my_dataset/view_foo",
                dry_run=True,
                yes=True,
            )

        mock_bq.run_query.assert_not_called()
        mock_write.assert_not_called()


# ---------------------------------------------------------------------------
# materialize_resource — execution path
# ---------------------------------------------------------------------------


class TestMaterializeResourceExecution:
    """Verify execution path calls run_query and targeted auto-pull."""

    def test_run_query_and_auto_pull(self, tmp_path: Path) -> None:
        """Execution calls run_query and then writes the model YAML."""
        config = _make_config()
        config_path = tmp_path / "bq_sync.toml"
        config_path.write_text("[project]\n[sync]\n")

        mock_table = MagicMock()

        with (
            patch("bq_sync.materialize.resolve_output_dir", return_value=tmp_path),
            patch("bq_sync.materialize.bq_client") as mock_bq,
            patch("bq_sync.materialize.write_model_yaml") as mock_write,
        ):
            mock_bq.get_table_info.return_value = mock_table
            materialize_resource(
                config,
                config_path,
                "my-project/my_dataset/view_foo",
                dry_run=False,
                yes=True,
            )

        # DDL executed.
        mock_bq.run_query.assert_called_once()
        ddl_arg = mock_bq.run_query.call_args[0][1]
        assert "CREATE OR REPLACE TABLE" in ddl_arg
        assert "materialize_foo" in ddl_arg

        # Auto-pull: metadata fetched and YAML written.
        mock_bq.get_table_info.assert_called_once_with(
            "my-project", "my_dataset", "materialize_foo"
        )
        mock_write.assert_called_once()

    def test_custom_target_name(self, tmp_path: Path) -> None:
        """``--target`` overrides the default naming."""
        config = _make_config()
        config_path = tmp_path / "bq_sync.toml"
        config_path.write_text("[project]\n[sync]\n")

        with (
            patch("bq_sync.materialize.resolve_output_dir", return_value=tmp_path),
            patch("bq_sync.materialize.bq_client") as mock_bq,
            patch("bq_sync.materialize.write_model_yaml"),
        ):
            mock_bq.get_table_info.return_value = MagicMock()
            materialize_resource(
                config,
                config_path,
                "my-project/my_dataset/view_foo",
                dry_run=False,
                yes=True,
                target_name="my_custom_table",
            )

        ddl_arg = mock_bq.run_query.call_args[0][1]
        assert "my_custom_table" in ddl_arg
        mock_bq.get_table_info.assert_called_once_with(
            "my-project", "my_dataset", "my_custom_table"
        )


# ---------------------------------------------------------------------------
# CLI parsing
# ---------------------------------------------------------------------------


class TestMaterializeCLIParsing:
    """Verify ``materialize`` subcommand argument parsing."""

    def test_resource_positional(self) -> None:
        """Resource path is captured as positional argument."""
        parser = _build_parser()
        args = parser.parse_args(["materialize", "proj/ds/view_foo"])

        assert args.command == "materialize"
        assert args.resource == "proj/ds/view_foo"

    def test_target_flag(self) -> None:
        """``--target`` sets custom target name."""
        parser = _build_parser()
        args = parser.parse_args(
            ["materialize", "proj/ds/view_foo", "--target", "my_table"]
        )

        assert args.target == "my_table"

    def test_dry_run_flag(self) -> None:
        """``--dry-run`` is supported."""
        parser = _build_parser()
        args = parser.parse_args(["materialize", "proj/ds/view_foo", "--dry-run"])

        assert args.dry_run is True

    def test_yes_flag(self) -> None:
        """``-y`` skips confirmation."""
        parser = _build_parser()
        args = parser.parse_args(["materialize", "proj/ds/view_foo", "-y"])

        assert args.yes is True

    def test_defaults(self) -> None:
        """Default values are correct."""
        parser = _build_parser()
        args = parser.parse_args(["materialize", "proj/ds/view_foo"])

        assert args.target is None
        assert args.dry_run is False
        assert args.yes is False
