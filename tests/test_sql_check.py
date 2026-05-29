"""Tests for the ``sql_check`` module."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from bq_sync.sql_check import (
    CheckSummary,
    _find_routine_model_yaml,
    _is_js_routine,
    _map_bq_type,
    _parse_routine_model_yaml,
    _parse_yaml_name,
    _parse_yaml_schema,
    _strip_header,
    build_catalog,
    check_file,
    check_files,
    check_js_contract,
    check_js_routine,
    check_js_syntax,
    check_lineage,
    check_references,
    check_syntax,
    discover_sql_files,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def tmp_project(tmp_path: Path) -> Path:
    """Create a minimal project tree with model YAMLs."""
    project_dir = tmp_path / "my_project"
    dataset_dir = project_dir / "my_dataset"

    # Models directory with table schemas.
    models_dir = dataset_dir / "models"
    models_dir.mkdir(parents=True)

    (models_dir / "users.yaml").write_text(
        textwrap.dedent("""\
            name: users
            description: "User table"
            schema:
              - name: id  type: INTEGER  mode: REQUIRED  description: "PK"
              - name: email  type: STRING  mode: NULLABLE  description: ""
              - name: active  type: BOOLEAN  mode: NULLABLE  description: ""
        """),
        encoding="utf-8",
    )

    (models_dir / "orders.yaml").write_text(
        textwrap.dedent("""\
            name: orders
            description: "Order table"
            schema:
              - name: order_id  type: INTEGER  mode: REQUIRED  description: ""
              - name: user_id  type: INTEGER  mode: NULLABLE  description: ""
              - name: total  type: FLOAT64  mode: NULLABLE  description: ""
        """),
        encoding="utf-8",
    )

    # Views directory with SQL files.
    views_dir = dataset_dir / "views"
    views_dir.mkdir(parents=True)

    (views_dir / "active_users.sql").write_text(
        "SELECT id, email FROM `my_project.my_dataset.users`"
        " WHERE active = TRUE\n",
        encoding="utf-8",
    )

    (views_dir / "broken.sql").write_text(
        "SELECT FROM WHERE\n",
        encoding="utf-8",
    )

    # Routines directory.
    routines_dir = dataset_dir / "routines"
    routines_dir.mkdir(parents=True)

    (routines_dir / "calc.sql").write_text(
        textwrap.dedent("""\
            -- Routine: calc
            -- Language: SQL

            DECLARE x INT64 DEFAULT 0;
            INSERT INTO `my_project.my_dataset.orders`
            SELECT 1 AS order_id, 42 AS user_id, 99.9 AS total;
        """),
        encoding="utf-8",
    )

    (routines_dir / "transform.sql").write_text(
        textwrap.dedent("""\
            -- Routine: transform
            -- Language: JAVASCRIPT

            function transform(x) {
              return x * 2;
            }
        """),
        encoding="utf-8",
    )

    (routines_dir / "broken_js.sql").write_text(
        textwrap.dedent("""\
            -- Routine: broken_js
            -- Language: JAVASCRIPT

            function bad(x { return x; }
        """),
        encoding="utf-8",
    )

    # Routine model YAML.
    routine_models_dir = routines_dir / "models"
    routine_models_dir.mkdir(parents=True)

    (routine_models_dir / "transform.yaml").write_text(
        textwrap.dedent("""\
            name: transform
            description: "Transforms input"
            language: JAVASCRIPT
            return_type: FLOAT64
            arguments:
              - name: x  type: FLOAT64  mode: IN
        """),
        encoding="utf-8",
    )

    # Saved queries directory (project-level).
    saved_dir = project_dir / "saved_queries"
    saved_dir.mkdir(parents=True)

    (saved_dir / "monthly.sql").write_text(
        textwrap.dedent("""\
            -- Saved Query: monthly

            SELECT order_id, total
            FROM `my_project.my_dataset.orders`
            WHERE total > 100
        """),
        encoding="utf-8",
    )

    return project_dir


@pytest.fixture()
def catalog(tmp_project: Path) -> dict:
    """Pre-built catalog from the test project."""
    return build_catalog(tmp_project, "my_project")


# ---------------------------------------------------------------------------
# Helper tests
# ---------------------------------------------------------------------------


class TestStripHeader:
    """Tests for ``_strip_header``."""

    def test_removes_routine_header(self) -> None:
        sql = "-- Routine: foo\n-- Language: SQL\n\nSELECT 1"
        assert _strip_header(sql) == "SELECT 1"

    def test_removes_saved_query_header(self) -> None:
        sql = "-- Saved Query: bar\n\nSELECT 2"
        assert _strip_header(sql) == "SELECT 2"

    def test_preserves_body(self) -> None:
        sql = "SELECT 1\n-- comment inside"
        assert _strip_header(sql) == sql

    def test_empty_input(self) -> None:
        assert _strip_header("") == ""

    def test_only_comments(self) -> None:
        assert _strip_header("-- only comments\n-- more").strip() == ""


class TestIsJsRoutine:
    """Tests for ``_is_js_routine``."""

    def test_javascript(self) -> None:
        sql = "-- Routine: foo\n-- Language: JAVASCRIPT\n\nreturn 1;"
        assert _is_js_routine(sql) is True

    def test_js(self) -> None:
        sql = "-- Routine: foo\n-- Language: JS\n\nreturn 1;"
        assert _is_js_routine(sql) is True

    def test_sql(self) -> None:
        sql = "-- Routine: foo\n-- Language: SQL\n\nSELECT 1"
        assert _is_js_routine(sql) is False

    def test_no_header(self) -> None:
        assert _is_js_routine("SELECT 1") is False


class TestParseYamlSchema:
    """Tests for ``_parse_yaml_schema``."""

    def test_parses_schema(self) -> None:
        yaml = textwrap.dedent("""\
            name: test
            schema:
              - name: id  type: INTEGER  mode: REQUIRED  description: ""
              - name: val  type: FLOAT64  mode: NULLABLE  description: ""
        """)
        fields = _parse_yaml_schema(yaml)
        assert len(fields) == 2
        assert fields[0] == {"name": "id", "type": "INTEGER"}
        assert fields[1] == {"name": "val", "type": "FLOAT64"}

    def test_empty_schema(self) -> None:
        yaml = "name: test\n"
        assert _parse_yaml_schema(yaml) == []


class TestParseYamlName:
    """Tests for ``_parse_yaml_name``."""

    def test_extracts_name(self) -> None:
        yaml = "name: my_table\ndescription: foo\n"
        assert _parse_yaml_name(yaml) == "my_table"

    def test_missing_name(self) -> None:
        assert _parse_yaml_name("description: foo\n") == ""


class TestMapBqType:
    """Tests for ``_map_bq_type``."""

    def test_scalar_types(self) -> None:
        assert _map_bq_type("STRING") == "string"
        assert _map_bq_type("INT64") == "int64"
        assert _map_bq_type("FLOAT64") == "float64"
        assert _map_bq_type("BOOL") == "bool"

    def test_case_insensitive(self) -> None:
        assert _map_bq_type("string") == "string"
        assert _map_bq_type("Int64") == "int64"

    def test_array_type(self) -> None:
        assert _map_bq_type("ARRAY<STRING>") == "array"

    def test_struct_type(self) -> None:
        assert _map_bq_type("STRUCT<x INT64, y STRING>") == "struct"

    def test_nested_array_struct(self) -> None:
        assert _map_bq_type("ARRAY<STRUCT<a INT64>>") == "array"

    def test_unknown_passthrough(self) -> None:
        assert _map_bq_type("INTERVAL") == "interval"


# ---------------------------------------------------------------------------
# Catalog tests
# ---------------------------------------------------------------------------


class TestBuildCatalog:
    """Tests for ``build_catalog``."""

    def test_builds_from_yaml(self, tmp_project: Path) -> None:
        catalog = build_catalog(tmp_project, "my_project")
        objects = catalog["schema_objects"]
        assert len(objects) == 2

        names = {obj["name"] for obj in objects}
        assert "my_project.my_dataset.users" in names
        assert "my_project.my_dataset.orders" in names

    def test_column_types(self, catalog: dict) -> None:
        users_obj = next(
            o for o in catalog["schema_objects"]
            if o["name"] == "my_project.my_dataset.users"
        )
        columns = users_obj["kind"]["table"]["columns"]
        col_map = {c["name"]: c["dtype"] for c in columns}
        assert col_map["id"] == "int64"
        assert col_map["email"] == "string"
        assert col_map["active"] == "bool"

    def test_empty_dir(self, tmp_path: Path) -> None:
        catalog = build_catalog(tmp_path, "proj")
        assert catalog["schema_objects"] == []


# ---------------------------------------------------------------------------
# Level 1: Syntax tests
# ---------------------------------------------------------------------------


class TestCheckSyntax:
    """Tests for ``check_syntax``."""

    def test_valid_select(self, tmp_path: Path) -> None:
        result = check_syntax(
            "SELECT 1 AS x", tmp_path / "test.sql"
        )
        assert result.level != "error"
        assert result.errors == []

    def test_invalid_sql(self, tmp_path: Path) -> None:
        result = check_syntax(
            "SELECT FROM WHERE", tmp_path / "bad.sql"
        )
        assert result.level == "error"
        assert len(result.errors) >= 1

    def test_procedural_sql(self, tmp_path: Path) -> None:
        sql = (
            "DECLARE x INT64 DEFAULT 0;\n"
            "INSERT INTO `p.d.t` SELECT 1 AS id;"
        )
        result = check_syntax(sql, tmp_path / "proc.sql")
        assert result.level != "error", (
            f"Procedural SQL should parse without error: {result.errors}"
        )

    def test_multistatement(self, tmp_path: Path) -> None:
        sql = "SELECT 1; SELECT 2; SELECT 3;"
        result = check_syntax(sql, tmp_path / "multi.sql")
        assert result.level != "error"


# ---------------------------------------------------------------------------
# Level 2: References tests
# ---------------------------------------------------------------------------


class TestCheckReferences:
    """Tests for ``check_references``."""

    def test_resolved_table(self, tmp_path: Path, catalog: dict) -> None:
        sql = "SELECT id FROM `my_project.my_dataset.users`"
        result = check_references(sql, tmp_path / "t.sql", catalog)
        assert "my_project.my_dataset.users" in result.tables_resolved
        assert result.tables_unresolved == []

    def test_unresolved_table(
        self, tmp_path: Path, catalog: dict
    ) -> None:
        sql = "SELECT id FROM `other_project.other.table`"
        result = check_references(sql, tmp_path / "t.sql", catalog)
        assert "other_project.other.table" in result.tables_unresolved
        assert result.level == "warning"

    def test_syntax_error_blocks_references(
        self, tmp_path: Path, catalog: dict
    ) -> None:
        result = check_references(
            "SELECT FROM WHERE", tmp_path / "bad.sql", catalog
        )
        assert result.level == "error"


# ---------------------------------------------------------------------------
# Level 3: Lineage tests
# ---------------------------------------------------------------------------


class TestCheckLineage:
    """Tests for ``check_lineage``."""

    def test_valid_lineage(self, tmp_path: Path, catalog: dict) -> None:
        sql = "SELECT id, email FROM `my_project.my_dataset.users`"
        result = check_lineage(sql, tmp_path / "t.sql", catalog)
        assert result.level != "error"
        assert "my_project.my_dataset.users" in result.tables_resolved

    def test_unresolved_table_in_lineage(
        self, tmp_path: Path, catalog: dict
    ) -> None:
        sql = "SELECT x FROM `unknown.ds.tbl`"
        result = check_lineage(sql, tmp_path / "t.sql", catalog)
        # Should not be an error — just info.
        assert result.level != "error"

    def test_syntax_error_blocks_lineage(
        self, tmp_path: Path, catalog: dict
    ) -> None:
        result = check_lineage(
            "SELECT FROM WHERE", tmp_path / "bad.sql", catalog
        )
        assert result.level == "error"


# ---------------------------------------------------------------------------
# Orchestrator tests
# ---------------------------------------------------------------------------


class TestCheckFile:
    """Tests for ``check_file``."""

    def test_valid_view(self, tmp_project: Path) -> None:
        sql_path = tmp_project / "my_dataset" / "views" / "active_users.sql"
        result = check_file(
            sql_path, tmp_project, "my_project"
        )
        assert result.level != "error"

    def test_broken_view(self, tmp_project: Path) -> None:
        sql_path = tmp_project / "my_dataset" / "views" / "broken.sql"
        result = check_file(
            sql_path, tmp_project, "my_project"
        )
        assert result.level == "error"
        assert len(result.errors) >= 1

    def test_js_routine_validated(self, tmp_project: Path) -> None:
        """JS routines are now validated, not skipped."""
        sql_path = (
            tmp_project / "my_dataset" / "routines" / "transform.sql"
        )
        result = check_file(
            sql_path, tmp_project, "my_project"
        )
        # Valid JS with matching contract → no error.
        assert result.level != "error", (
            f"Valid JS routine should not error: {result.errors}"
        )

    def test_broken_js_routine(self, tmp_project: Path) -> None:
        sql_path = (
            tmp_project / "my_dataset" / "routines" / "broken_js.sql"
        )
        result = check_file(
            sql_path, tmp_project, "my_project"
        )
        assert result.level == "error"
        assert len(result.errors) >= 1

    def test_procedural_routine(self, tmp_project: Path) -> None:
        sql_path = (
            tmp_project / "my_dataset" / "routines" / "calc.sql"
        )
        result = check_file(
            sql_path, tmp_project, "my_project"
        )
        # Should not be a syntax error — inbq handles procedural SQL.
        assert result.level != "error", (
            f"Procedural routine should not error: {result.errors}"
        )

    def test_saved_query(self, tmp_project: Path) -> None:
        sql_path = tmp_project / "saved_queries" / "monthly.sql"
        result = check_file(
            sql_path, tmp_project, "my_project"
        )
        assert result.level != "error"


class TestCheckFiles:
    """Tests for ``check_files`` batch orchestrator."""

    def test_batch_summary(self, tmp_project: Path) -> None:
        files = [
            tmp_project / "my_dataset" / "views" / "active_users.sql",
            tmp_project / "my_dataset" / "views" / "broken.sql",
        ]
        summary = check_files(files, tmp_project, "my_project")
        assert isinstance(summary, CheckSummary)
        assert summary.failed >= 1
        assert summary.passed >= 1
        # Disjoint: passed + failed + warned == total files.
        assert (
            summary.passed + summary.failed + summary.warned
            == len(files)
        )

    def test_empty_batch(self, tmp_project: Path) -> None:
        summary = check_files([], tmp_project, "my_project")
        assert summary.failed == 0
        assert summary.passed == 0
        assert summary.warned == 0

    def test_disjoint_counts(self, tmp_project: Path) -> None:
        """Verify that passed, failed, warned are disjoint buckets."""
        # 5 files: 1 broken (error), 1 JS skip (info), others (info/warn)
        files = discover_sql_files(tmp_project)
        summary = check_files(files, tmp_project, "my_project")
        total = len(files)
        assert summary.passed + summary.failed + summary.warned == total


# ---------------------------------------------------------------------------
# Discovery tests
# ---------------------------------------------------------------------------


class TestDiscoverSqlFiles:
    """Tests for ``discover_sql_files``."""

    def test_finds_sql_files(self, tmp_project: Path) -> None:
        files = discover_sql_files(tmp_project)
        names = {f.name for f in files}
        assert "active_users.sql" in names
        assert "broken.sql" in names
        assert "calc.sql" in names
        assert "transform.sql" in names
        assert "broken_js.sql" in names
        assert "monthly.sql" in names

    def test_empty_dir(self, tmp_path: Path) -> None:
        assert discover_sql_files(tmp_path) == []


# ---------------------------------------------------------------------------
# Routine model YAML tests
# ---------------------------------------------------------------------------


class TestParseRoutineModelYaml:
    """Tests for ``_parse_routine_model_yaml``."""

    def test_full_model(self) -> None:
        yaml = textwrap.dedent("""\
            name: transform
            description: "Transforms input"
            language: JAVASCRIPT
            return_type: FLOAT64
            arguments:
              - name: x  type: FLOAT64  mode: IN
              - name: y  type: STRING  mode: IN
        """)
        model = _parse_routine_model_yaml(yaml)
        assert model["return_type"] == "FLOAT64"
        assert len(model["arguments"]) == 2
        assert model["arguments"][0] == {"name": "x", "type": "FLOAT64"}
        assert model["arguments"][1] == {"name": "y", "type": "STRING"}

    def test_no_return_type(self) -> None:
        yaml = textwrap.dedent("""\
            name: proc
            language: JAVASCRIPT
            arguments:
              - name: x  type: INT64  mode: IN
        """)
        model = _parse_routine_model_yaml(yaml)
        assert model["return_type"] is None
        assert len(model["arguments"]) == 1

    def test_no_arguments(self) -> None:
        yaml = textwrap.dedent("""\
            name: noargs
            language: JAVASCRIPT
            return_type: STRING
        """)
        model = _parse_routine_model_yaml(yaml)
        assert model["return_type"] == "STRING"
        assert model["arguments"] == []

    def test_empty_yaml(self) -> None:
        model = _parse_routine_model_yaml("")
        assert model["return_type"] is None
        assert model["arguments"] == []


class TestFindRoutineModelYaml:
    """Tests for ``_find_routine_model_yaml``."""

    def test_finds_existing_model(self, tmp_project: Path) -> None:
        sql_path = (
            tmp_project / "my_dataset" / "routines" / "transform.sql"
        )
        yaml_path = _find_routine_model_yaml(sql_path)
        assert yaml_path is not None
        assert yaml_path.name == "transform.yaml"

    def test_returns_none_for_missing(self, tmp_project: Path) -> None:
        sql_path = (
            tmp_project / "my_dataset" / "routines" / "nonexistent.sql"
        )
        assert _find_routine_model_yaml(sql_path) is None


# ---------------------------------------------------------------------------
# JavaScript validation tests
# ---------------------------------------------------------------------------


class TestCheckJsSyntax:
    """Tests for ``check_js_syntax``."""

    def test_valid_function(self, tmp_path: Path) -> None:
        js = "function f(x, y) { return x * y; }"
        result = check_js_syntax(js, tmp_path / "test.sql")
        assert result.level != "error"
        assert result.errors == []

    def test_valid_bare_return(self, tmp_path: Path) -> None:
        """BQ JS UDFs can use bare return (no function wrapper)."""
        js = "return x + y;"
        result = check_js_syntax(js, tmp_path / "test.sql")
        assert result.level != "error"

    def test_valid_arrow_function(self, tmp_path: Path) -> None:
        js = "const double = (x) => x * 2;"
        result = check_js_syntax(js, tmp_path / "test.sql")
        assert result.level != "error"

    def test_valid_multiline(self, tmp_path: Path) -> None:
        js = textwrap.dedent("""\
            function transform(x) {
              var result = x * 2;
              if (result > 100) {
                result = 100;
              }
              return result;
            }
        """)
        result = check_js_syntax(js, tmp_path / "test.sql")
        assert result.level != "error"

    def test_missing_paren(self, tmp_path: Path) -> None:
        js = "function f(x { return x; }"
        result = check_js_syntax(js, tmp_path / "bad.sql")
        assert result.level == "error"
        assert len(result.errors) >= 1

    def test_unexpected_token(self, tmp_path: Path) -> None:
        js = "function f() { return +++; }"
        result = check_js_syntax(js, tmp_path / "bad.sql")
        assert result.level == "error"

    def test_empty_body(self, tmp_path: Path) -> None:
        js = ""
        result = check_js_syntax(js, tmp_path / "empty.sql")
        assert result.level != "error"

    def test_error_has_line_info(self, tmp_path: Path) -> None:
        js = "function f(x {\n  return x;\n}"
        result = check_js_syntax(js, tmp_path / "bad.sql")
        assert result.level == "error"
        assert any("[line" in e for e in result.errors)


class TestCheckJsContract:
    """Tests for ``check_js_contract``."""

    def test_matching_contract(self, tmp_path: Path) -> None:
        """No warnings when function params match YAML args."""
        js = "function transform(x) { return x * 2; }"
        yaml_path = tmp_path / "model.yaml"
        yaml_path.write_text(
            textwrap.dedent("""\
                name: transform
                return_type: FLOAT64
                arguments:
                  - name: x  type: FLOAT64  mode: IN
            """),
            encoding="utf-8",
        )
        result = check_js_contract(js, tmp_path / "t.sql", yaml_path)
        assert result.level != "warning"
        assert result.warnings == []

    def test_missing_return_statement(self, tmp_path: Path) -> None:
        """Warn if return_type is set but body has no return."""
        js = "var x = 42;"
        yaml_path = tmp_path / "model.yaml"
        yaml_path.write_text(
            textwrap.dedent("""\
                name: test
                return_type: FLOAT64
            """),
            encoding="utf-8",
        )
        result = check_js_contract(js, tmp_path / "t.sql", yaml_path)
        assert result.level == "warning"
        assert any("return" in w.lower() for w in result.warnings)

    def test_arg_count_mismatch(self, tmp_path: Path) -> None:
        """Warn if JS function param count differs from YAML args."""
        js = "function f(a, b, c) { return a + b + c; }"
        yaml_path = tmp_path / "model.yaml"
        yaml_path.write_text(
            textwrap.dedent("""\
                name: f
                return_type: FLOAT64
                arguments:
                  - name: a  type: FLOAT64  mode: IN
                  - name: b  type: FLOAT64  mode: IN
            """),
            encoding="utf-8",
        )
        result = check_js_contract(js, tmp_path / "t.sql", yaml_path)
        assert result.level == "warning"
        assert any("argument" in w.lower() for w in result.warnings)

    def test_bare_return_no_function(self, tmp_path: Path) -> None:
        """Bare return with args — no param mismatch warning."""
        js = "return x + y;"
        yaml_path = tmp_path / "model.yaml"
        yaml_path.write_text(
            textwrap.dedent("""\
                name: add
                return_type: FLOAT64
                arguments:
                  - name: x  type: FLOAT64  mode: IN
                  - name: y  type: FLOAT64  mode: IN
            """),
            encoding="utf-8",
        )
        result = check_js_contract(js, tmp_path / "t.sql", yaml_path)
        # No function_declaration → arg count check is skipped.
        assert not any("argument" in w.lower() for w in result.warnings)

    def test_no_model_yaml(self, tmp_path: Path) -> None:
        """No warnings if model YAML is absent."""
        js = "function f(x) { return x; }"
        result = check_js_contract(
            js, tmp_path / "t.sql", model_yaml_path=None
        )
        assert result.level != "warning"

    def test_no_return_type_no_warning(self, tmp_path: Path) -> None:
        """No warning about missing return if return_type is unset."""
        js = "console.log('hello');"
        yaml_path = tmp_path / "model.yaml"
        yaml_path.write_text(
            textwrap.dedent("""\
                name: side_effect
                language: JAVASCRIPT
            """),
            encoding="utf-8",
        )
        result = check_js_contract(js, tmp_path / "t.sql", yaml_path)
        assert not any("return" in w.lower() for w in result.warnings)


class TestCheckJsRoutine:
    """Tests for the ``check_js_routine`` orchestrator."""

    def test_valid_routine(self, tmp_project: Path) -> None:
        sql_path = (
            tmp_project / "my_dataset" / "routines" / "transform.sql"
        )
        raw = sql_path.read_text(encoding="utf-8")
        result = check_js_routine(raw, sql_path)
        assert result.level != "error"

    def test_broken_routine(self, tmp_project: Path) -> None:
        sql_path = (
            tmp_project / "my_dataset" / "routines" / "broken_js.sql"
        )
        raw = sql_path.read_text(encoding="utf-8")
        result = check_js_routine(raw, sql_path)
        assert result.level == "error"

    def test_empty_body(self, tmp_path: Path) -> None:
        raw = "-- Routine: empty\n-- Language: JAVASCRIPT\n\n"
        result = check_js_routine(raw, tmp_path / "empty.sql")
        assert result.level == "info"
        assert any("empty" in m.lower() for m in result.info)
