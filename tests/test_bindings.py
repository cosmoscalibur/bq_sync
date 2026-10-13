"""Tests for the ``bq_sync`` Python bindings (Rust extension module).

Core logic is covered by the Rust unit tests (``cargo test``); these tests
check the Python-facing API surface: conversions, defaults, errors and the
console entry point.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

import bq_sync
from bq_sync import cli

CONFIG = textwrap.dedent("""\
    [project]
    id = "my-project"
    default_region = "us-east1"

    [sync]
    datasets = ["my_dataset"]
""")


@pytest.fixture()
def project(tmp_path: Path) -> Path:
    """Config file plus a minimal output tree; returns the config path."""
    config = tmp_path / "bq_sync.toml"
    config.write_text(CONFIG, encoding="utf-8")
    ds = tmp_path / "my-project" / "my_dataset"
    (ds / "models").mkdir(parents=True)
    (ds / "views").mkdir()
    (ds / "models" / "users.yaml").write_text(
        'name: users\ndescription: "Users"\nschema:\n'
        '  - name: id  type: INTEGER  mode: REQUIRED  description: "PK"\n'
        '  - name: email  type: STRING  mode: NULLABLE  description: ""\n',
        encoding="utf-8",
    )
    (ds / "views" / "active.sql").write_text(
        "SELECT id, email FROM `my-project.my_dataset.users`\n", encoding="utf-8"
    )
    return config


def test_version() -> None:
    assert bq_sync.__version__.count(".") == 2


def test_humanize_bytes() -> None:
    assert bq_sync.humanize_bytes(None) == ""
    assert bq_sync.humanize_bytes(512) == "512 B"
    assert bq_sync.humanize_bytes(5 * 1024**3) == "5.0 GiB"


def test_parse_resource_path() -> None:
    assert bq_sync.parse_resource_path("p/d/models/t.yaml") == ("p", "d", "t")
    assert bq_sync.parse_resource_path("p/d/v.sql") == ("p", "d", "v")
    with pytest.raises(bq_sync.BqSyncError):
        bq_sync.parse_resource_path("p/d")


def test_config_helpers(project: Path) -> None:
    sub = project.parent / "my-project" / "my_dataset"
    assert bq_sync.discover_config(sub) == project.resolve()
    assert bq_sync.load_config(project) == {
        "project": {"id": "my-project", "default_region": "us-east1"},
        "datasets": ["my_dataset"],
        "output_dir": ".",
    }
    expected = project.parent.resolve() / "my-project"
    assert bq_sync.resolve_output_dir(str(project)) == expected


def test_config_errors(tmp_path: Path) -> None:
    with pytest.raises(bq_sync.BqSyncError):
        bq_sync.discover_config(tmp_path)
    with pytest.raises(OSError):
        bq_sync.load_config(tmp_path / "missing.toml")
    bad = tmp_path / "bad.toml"
    bad.write_text("[project]\n", encoding="utf-8")
    with pytest.raises(bq_sync.BqSyncError):
        bq_sync.load_config(bad)


def test_readers(tmp_path: Path) -> None:
    routine = tmp_path / "fn.sql"
    routine.write_text("-- Routine: fn\n-- Language: JAVASCRIPT\n\nreturn 1;")
    assert bq_sync.read_routine_sql(routine) == {
        "name": "fn",
        "body": "return 1;",
        "language": "JAVASCRIPT",
    }

    saved = tmp_path / "q.sql"
    saved.write_text("-- Saved Query: Monthly\n\nSELECT 1\n")
    assert bq_sync.read_saved_query_sql(saved) == {
        "name": "Monthly",
        "sql": "SELECT 1\n",
    }
    assert bq_sync.read_view_sql(saved)["name"] == "q"

    model = tmp_path / "t.yaml"
    model.write_text(
        'name: t\ndescription: "Tabla de declaración"\nschema:\n'
        '  - name: id  type: INT64  mode: NULLABLE  description: "Id"\n',
        encoding="utf-8",
    )
    assert bq_sync.read_model_yaml(model) == {
        "name": "t",
        "description": "Tabla de declaración",
        "field_descriptions": {"id": "Id"},
    }

    rmodel = tmp_path / "fn.yaml"
    rmodel.write_text(
        "name: fn\nlanguage: SQL\nreturn_type: INT64\narguments:\n"
        "  - name: x  type: INT64  mode: IN\n"
    )
    parsed = bq_sync.read_routine_model_yaml(rmodel)
    assert parsed["return_type"] == "INT64"
    assert parsed["arguments"] == [{"name": "x", "type": "INT64", "mode": "IN"}]


def test_parse_schedule_from_file(tmp_path: Path) -> None:
    f = tmp_path / "daily.sql"
    f.write_text("-- Scheduled Query: daily\n-- Schedule: every 24 hours\n\nSELECT 1")
    assert bq_sync.parse_schedule_from_file(f) == ("daily", "every 24 hours")
    f.write_text("SELECT 1")
    with pytest.raises(bq_sync.BqSyncError, match="Scheduled Query"):
        bq_sync.parse_schedule_from_file(f)


def test_check(project: Path) -> None:
    root = project.parent / "my-project"
    assert [p.name for p in bq_sync.discover_sql_files(root)] == ["active.sql"]

    summary = bq_sync.check(config=project)
    assert (summary["passed"], summary["failed"]) == (1, 0)
    result = summary["results"][0]
    assert result["level"] == "info"
    assert result["tables_resolved"] == ["my-project.my_dataset.users"]
    assert isinstance(result["path"], Path)

    broken = root / "my_dataset" / "views" / "broken.sql"
    broken.write_text("SELECT FROM WHERE")
    summary = bq_sync.check_files([broken], root, "my-project")
    assert summary["failed"] == 1
    assert summary["results"][0]["errors"]


def test_cli_main_exit_codes(project: Path) -> None:
    assert bq_sync._bq_sync.main(["--version"]) == 0
    assert bq_sync._bq_sync.main(["fetch"]) == 2  # usage error
    assert bq_sync._bq_sync.main(["check", "--config", str(project)]) == 0

    with pytest.raises(SystemExit) as exc:
        cli.main(["check", "--config", str(project.parent / "missing.toml")])
    assert exc.value.code == 1
