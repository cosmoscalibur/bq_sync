"""SQL and JavaScript validation for BigQuery resources.

Uses ``inbq`` (Rust + PyO3) for parsing BigQuery SQL and extracting
column-level lineage, and ``tree-sitter`` for JavaScript UDF syntax
validation.  Analysis levels:

1. **Syntax** — parse SQL/JS and report errors.
2. **References** — extract referenced tables and verify against the
   local model catalog.
3. **Lineage** — trace column-level data flow and report unresolvable
   columns.
4. **Contract** (JS only) — verify return statements and argument
   counts against the routine model YAML.

This module is invoked by the ``bq-sync check`` CLI subcommand and is
**not** coupled to the push workflow.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import inbq
import inbq.ast_nodes as ast_nodes
from tree_sitter import Language, Parser

try:
    import tree_sitter_javascript as _tsjs

    _JS_LANGUAGE = Language(_tsjs.language())
except Exception:  # pragma: no cover – optional at import time
    _JS_LANGUAGE = None

logger = logging.getLogger(__name__)

# Header patterns (must match those in ``writers.py``).
_ROUTINE_HEADER_RE = re.compile(
    r"^--\s*Routine:\s*.+$\n--\s*Language:\s*(.+)$", re.MULTILINE
)
_SAVED_QUERY_HEADER_RE = re.compile(r"^--\s*Saved Query:\s*.+$", re.MULTILINE)

# Directories that contain SQL files eligible for validation.
_SQL_DIRS = {"views", "routines", "saved_queries"}


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


@dataclass
class CheckResult:
    """Validation result for a single SQL file.

    Attributes:
        path: Absolute path to the checked file.
        level: Overall severity (``"error"``, ``"warning"``, or ``"info"``).
        errors: Parse or validation errors (make ``level`` ``"error"``).
        warnings: Non-blocking issues (unresolved columns, unknown tables).
        info: Informational messages (cross-project refs, skipped files).
        tables_referenced: Fully-qualified table names found in the SQL.
        tables_resolved: Subset of ``tables_referenced`` present in the
            local catalog.
        tables_unresolved: Subset not found in the catalog.
        columns_unresolved: Columns that could not be resolved during
            lineage analysis.
    """

    path: Path
    level: Literal["error", "warning", "info"] = "info"
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    info: list[str] = field(default_factory=list)
    tables_referenced: list[str] = field(default_factory=list)
    tables_resolved: list[str] = field(default_factory=list)
    tables_unresolved: list[str] = field(default_factory=list)
    columns_unresolved: list[str] = field(default_factory=list)


@dataclass
class CheckSummary:
    """Aggregate results for a batch of checked files.

    The three counters (``passed``, ``failed``, ``warned``) are
    **disjoint**: every file falls into exactly one bucket.

    Attributes:
        results: Per-file results.
        passed: Count of files without errors or warnings.
        failed: Count of files with errors.
        warned: Count of files with warnings but no errors.
    """

    results: list[CheckResult]
    passed: int = 0
    failed: int = 0
    warned: int = 0


# ---------------------------------------------------------------------------
# Catalog builder
# ---------------------------------------------------------------------------

# Mapping from BQ scalar types to inbq dtype strings.
_BQ_TYPE_MAP: dict[str, str] = {
    "STRING": "string",
    "BYTES": "bytes",
    "INTEGER": "int64",
    "INT64": "int64",
    "FLOAT": "float64",
    "FLOAT64": "float64",
    "NUMERIC": "numeric",
    "BIGNUMERIC": "bignumeric",
    "BOOLEAN": "bool",
    "BOOL": "bool",
    "TIMESTAMP": "timestamp",
    "DATE": "date",
    "TIME": "time",
    "DATETIME": "datetime",
    "GEOGRAPHY": "geography",
    "JSON": "json",
    "RECORD": "struct",
    "STRUCT": "struct",
}

# Regex to extract the base type from parameterized BQ types
# like ARRAY<STRING>, STRUCT<x INT64>, ARRAY<STRUCT<...>>.
_PARAMETERIZED_TYPE_RE = re.compile(r"^(ARRAY|STRUCT)\b", re.IGNORECASE)


def _map_bq_type(bq_type: str) -> str:
    """Map a BigQuery type string to an ``inbq`` dtype.

    Handles both scalar types (via ``_BQ_TYPE_MAP``) and parameterized
    types like ``ARRAY<STRING>`` or ``STRUCT<x INT64, y STRING>``.

    Args:
        bq_type: BigQuery type string (e.g. ``"INT64"``,
            ``"ARRAY<STRING>"``).

    Returns:
        Corresponding ``inbq`` dtype string.
    """
    upper = bq_type.strip().upper()
    # Direct scalar lookup.
    if upper in _BQ_TYPE_MAP:
        return _BQ_TYPE_MAP[upper]
    # Parameterized types: extract the base type.
    m = _PARAMETERIZED_TYPE_RE.match(upper)
    if m:
        base = m.group(1).upper()
        return _BQ_TYPE_MAP.get(base, base.lower())
    # Unknown type: pass through lowered.
    return bq_type.lower()


def _parse_yaml_schema(text: str) -> list[dict[str, str]]:
    """Extract schema fields from a model YAML file.

    Uses line-by-line parsing (no external YAML dep) matching the
    format produced by ``writers._format_schema_lines``.

    Args:
        text: Full YAML file content.

    Returns:
        List of ``{"name": ..., "type": ...}`` dicts.
    """
    fields: list[dict[str, str]] = []
    in_schema = False
    for line in text.splitlines():
        stripped = line.strip()
        if stripped == "schema:":
            in_schema = True
            continue
        if not in_schema:
            continue
        # End of schema block.
        if (
            not line.startswith(" ")
            and stripped
            and not stripped.startswith("-")
        ):
            break
        if stripped.startswith("- name:"):
            parts = stripped.split("  ")
            name = ""
            dtype = ""
            for part in parts:
                # Character-based strip: removes leading '-' and ' '.
                # Safe because BQ column names start with [a-zA-Z_].
                part = part.strip().lstrip("- ")
                if part.startswith("name:"):
                    name = part[len("name:") :].strip()
                elif part.startswith("type:"):
                    dtype = part[len("type:") :].strip()
            if name:
                fields.append({"name": name, "type": dtype})
    return fields


def _parse_yaml_name(text: str) -> str:
    """Extract the ``name:`` value from a model YAML."""
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("name:"):
            return stripped[len("name:") :].strip()
    return ""


def build_catalog(
    output_root: Path,
    project: str,
) -> dict:
    """Build an ``inbq``-compatible catalog from local YAML models.

    Scans ``<dataset>/models/*.yaml`` and ``<dataset>/externals/*.yaml``
    under *output_root* to create the schema catalog.

    Args:
        output_root: Project-scoped output directory (e.g.
            ``<base>/<project>``).
        project: GCP project ID.

    Returns:
        Catalog dict in the format expected by ``inbq.Pipeline``.
    """
    schema_objects: list[dict] = []

    yaml_globs = [
        output_root.glob("*/models/*.yaml"),
        output_root.glob("*/externals/*.yaml"),
    ]
    for glob in yaml_globs:
        for yaml_path in glob:
            text = yaml_path.read_text(encoding="utf-8")
            schema_fields = _parse_yaml_schema(text)
            if not schema_fields:
                continue
            table_name = _parse_yaml_name(text)
            if not table_name:
                continue
            # Dataset comes from the parent directory structure.
            dataset = yaml_path.parent.parent.name
            fqn = f"{project}.{dataset}.{table_name}"
            columns = []
            for f in schema_fields:
                bq_type = f.get("type", "STRING")
                dtype = _map_bq_type(bq_type)
                columns.append({"name": f["name"], "dtype": dtype})
            schema_objects.append(
                {
                    "name": fqn,
                    "kind": {"table": {"columns": columns}},
                }
            )

    logger.debug(
        "Built catalog with %d schema objects from %s",
        len(schema_objects),
        output_root,
    )
    return {"schema_objects": schema_objects}


# ---------------------------------------------------------------------------
# Pre-processing helpers
# ---------------------------------------------------------------------------


def _strip_header(sql: str) -> str:
    """Strip leading comment and blank lines from SQL text.

    Removes **all** contiguous leading ``--`` comments and blank lines
    until the first line of actual SQL.  This is broader than just
    writer-generated headers (``-- Routine:``, ``-- Saved Query:``),
    but is safe for validation because SQL parsers handle comments
    correctly — the stripped lines carry no semantic value for
    syntax, reference, or lineage analysis.

    Args:
        sql: Raw SQL file content.

    Returns:
        SQL body without leading comments or blank lines.
    """
    lines = sql.splitlines()
    start = 0
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("--") or stripped == "":
            start = i + 1
        else:
            break
    return "\n".join(lines[start:])




def _is_js_routine(sql: str) -> bool:
    """Return ``True`` if the SQL header indicates a JavaScript routine.

    Args:
        sql: Raw SQL file content.

    Returns:
        Whether the file is a JS routine.
    """
    m = _ROUTINE_HEADER_RE.search(sql)
    if m:
        return m.group(1).strip().upper() in ("JAVASCRIPT", "JS")
    return False


# ---------------------------------------------------------------------------
# Routine model YAML helpers
# ---------------------------------------------------------------------------


def _find_routine_model_yaml(sql_path: Path) -> Path | None:
    """Locate the routine model YAML for a given routine SQL file.

    Model YAMLs live in ``<dataset>/routines/models/<name>.yaml``.

    Args:
        sql_path: Absolute path to the routine ``.sql`` file.

    Returns:
        Path to the model YAML if it exists, ``None`` otherwise.
    """
    models_dir = sql_path.parent / "models"
    yaml_path = models_dir / f"{sql_path.stem}.yaml"
    if yaml_path.is_file():
        return yaml_path
    return None


def _parse_routine_model_yaml(
    text: str,
) -> dict:
    """Parse a routine model YAML into a metadata dict.

    Extracts ``return_type`` and ``arguments`` from the format
    produced by ``writers.write_routine_model_yaml``.

    Args:
        text: Full YAML file content.

    Returns:
        Dict with keys ``"return_type"`` (str or None) and
        ``"arguments"`` (list of ``{"name": ..., "type": ...}``).
    """
    result: dict = {"return_type": None, "arguments": []}
    in_arguments = False
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("return_type:"):
            result["return_type"] = stripped[len("return_type:"):].strip()
            continue
        if stripped == "arguments:":
            in_arguments = True
            continue
        if in_arguments:
            if (
                not line.startswith(" ")
                and stripped
                and not stripped.startswith("-")
            ):
                break
            if stripped.startswith("- name:"):
                parts = stripped.split("  ")
                name = ""
                dtype = ""
                for part in parts:
                    # Character-based strip: same as _parse_yaml_schema.
                    part = part.strip().lstrip("- ")
                    if part.startswith("name:"):
                        name = part[len("name:"):].strip()
                    elif part.startswith("type:"):
                        dtype = part[len("type:"):].strip()
                if name:
                    result["arguments"].append(
                        {"name": name, "type": dtype}
                    )
    return result


# ---------------------------------------------------------------------------
# JavaScript validation (tree-sitter)
# ---------------------------------------------------------------------------


def _walk_tree(node):
    """Yield all nodes in a tree-sitter tree (pre-order DFS).

    Args:
        node: Root tree-sitter node.

    Yields:
        Each node in the tree.
    """
    yield node
    for child in node.children:
        yield from _walk_tree(child)


def check_js_syntax(js_body: str, path: Path) -> CheckResult:
    """Validate JavaScript syntax using tree-sitter.

    Parses the JS body and searches for ``ERROR`` nodes in the
    concrete syntax tree.  Tree-sitter always produces a tree
    (even for invalid input), with errors as localized nodes.

    Args:
        js_body: JavaScript source code (header stripped).
        path: File path for error reporting.

    Returns:
        ``CheckResult`` with syntax errors if any.
    """
    result = CheckResult(path=path)

    if _JS_LANGUAGE is None:
        result.info.append(
            "tree-sitter-javascript not available; "
            "JS syntax check skipped"
        )
        return result

    parser = Parser(_JS_LANGUAGE)
    tree = parser.parse(js_body.encode("utf-8"))

    errors: list[str] = []
    for node in _walk_tree(tree.root_node):
        if node.type == "ERROR" or node.is_missing:
            row = node.start_point.row + 1
            col = node.start_point.column + 1
            snippet = (
                node.text.decode("utf-8", errors="replace")[:40]
                if node.text
                else ""
            )
            if node.is_missing:
                errors.append(
                    f"[line {row}, col {col}] "
                    f"Missing expected token: {node.type}"
                )
            else:
                errors.append(
                    f"[line {row}, col {col}] "
                    f"Syntax error near: {snippet!r}"
                )

    if errors:
        result.errors = errors
        result.level = "error"
    else:
        result.level = "info"

    return result


def check_js_contract(
    js_body: str,
    path: Path,
    model_yaml_path: Path | None = None,
) -> CheckResult:
    """Validate the JS routine contract against its model YAML.

    Checks:
    - Whether the body contains a ``return`` statement (expected for
      functions with a ``return_type``).
    - Whether the number of function parameters matches the argument
      count in the model YAML.

    Args:
        js_body: JavaScript source code (header stripped).
        path: File path for error reporting.
        model_yaml_path: Path to the routine model YAML (optional).

    Returns:
        ``CheckResult`` with contract warnings.
    """
    result = CheckResult(path=path)

    if _JS_LANGUAGE is None:
        return result

    parser = Parser(_JS_LANGUAGE)
    tree = parser.parse(js_body.encode("utf-8"))

    # Detect return statements.
    has_return = any(
        node.type == "return_statement"
        for node in _walk_tree(tree.root_node)
    )

    # Load model YAML if available.
    model: dict | None = None
    if model_yaml_path and model_yaml_path.is_file():
        text = model_yaml_path.read_text(encoding="utf-8")
        model = _parse_routine_model_yaml(text)

    if model:
        # Check return statement vs return_type.
        if model.get("return_type") and not has_return:
            result.warnings.append(
                f"Routine declares return_type "
                f"'{model['return_type']}' but JS body has no "
                f"return statement"
            )

        # Check argument count (heuristic — BQ JS UDFs receive args
        # as global variables, so function parameters are optional).
        yaml_args = model.get("arguments", [])
        func_nodes = [
            n for n in _walk_tree(tree.root_node)
            if n.type == "function_declaration"
        ]
        if func_nodes:
            params_node = next(
                (
                    n
                    for n in _walk_tree(func_nodes[0])
                    if n.type == "formal_parameters"
                ),
                None,
            )
            if params_node:
                js_params = [
                    n
                    for n in params_node.children
                    if n.type == "identifier"
                ]
                if len(js_params) != len(yaml_args):
                    result.warnings.append(
                        f"YAML declares {len(yaml_args)} "
                        f"argument(s) but JS function has "
                        f"{len(js_params)} parameter(s)"
                    )

    if result.warnings:
        result.level = "warning"
    else:
        result.level = "info"

    return result


def check_js_routine(
    raw_sql: str,
    path: Path,
) -> CheckResult:
    """Run all JS validation levels on a single routine file.

    Combines syntax check and contract validation results.

    Args:
        raw_sql: Raw file content (with header).
        path: Absolute path to the SQL file.

    Returns:
        Merged ``CheckResult``.
    """
    js_body = _strip_header(raw_sql)
    if not js_body.strip():
        return CheckResult(
            path=path,
            level="info",
            info=["Skipped: empty JS routine body"],
        )

    # Level 1: JS Syntax.
    syntax_result = check_js_syntax(js_body, path)
    if syntax_result.errors:
        return syntax_result

    # Level 1.5: Contract validation.
    model_path = _find_routine_model_yaml(path)
    contract_result = check_js_contract(
        js_body, path, model_yaml_path=model_path
    )

    # Merge results.
    result = CheckResult(path=path)
    result.info.extend(syntax_result.info)
    result.warnings.extend(contract_result.warnings)
    result.info.extend(contract_result.info)

    if result.errors:
        result.level = "error"
    elif result.warnings:
        result.level = "warning"
    else:
        result.level = "info"

    return result


# ---------------------------------------------------------------------------
# Level 1: Syntax check
# ---------------------------------------------------------------------------


def check_syntax(sql: str, path: Path) -> CheckResult:
    """Parse SQL and return syntax errors.

    Uses ``inbq.Pipeline`` with ``raise_exception_on_error=False``
    so that parse errors are captured rather than raised.

    Args:
        sql: SQL text to parse (headers already stripped).
        path: File path (for error reporting).

    Returns:
        ``CheckResult`` with errors populated if parsing fails.
    """
    result = CheckResult(path=path)

    pipeline = (
        inbq.Pipeline()
        .config(raise_exception_on_error=False)
        .parse()
    )
    output = inbq.run_pipeline([sql], pipeline=pipeline)
    ast = output.asts[0]

    if isinstance(ast, inbq.PipelineError):
        result.errors.append(ast.error)
        result.level = "error"
    else:
        result.level = "info"

    return result


# ---------------------------------------------------------------------------
# Level 2: Table references
# ---------------------------------------------------------------------------


def _extract_tables_from_ast(ast: ast_nodes.Ast) -> list[str]:
    """Walk the AST and collect fully-qualified table references.

    Searches for ``FromPathExpr`` nodes which represent table references
    in ``FROM`` and ``JOIN`` clauses.

    Args:
        ast: Parsed AST object from ``inbq``.

    Returns:
        Deduplicated list of table names found in FROM/JOIN clauses.
    """
    tables: set[str] = set()
    for node in ast.find_all(ast_nodes.FromPathExpr):
        if hasattr(node, "path") and node.path and node.path.name:
            tables.add(node.path.name)
    return sorted(tables)


def check_references(
    sql: str,
    path: Path,
    catalog: dict,
) -> CheckResult:
    """Parse SQL and verify table references against the local catalog.

    Args:
        sql: SQL text (headers stripped).
        path: File path for error reporting.
        catalog: Catalog dict built by ``build_catalog``.

    Returns:
        ``CheckResult`` with table resolution info and warnings for
        unresolved tables.
    """
    result = CheckResult(path=path)

    pipeline = (
        inbq.Pipeline()
        .config(raise_exception_on_error=False)
        .parse()
    )
    output = inbq.run_pipeline([sql], pipeline=pipeline)
    ast = output.asts[0]

    if isinstance(ast, inbq.PipelineError):
        result.errors.append(ast.error)
        result.level = "error"
        return result

    # Extract table references from the AST.
    tables = _extract_tables_from_ast(ast)
    result.tables_referenced = tables

    # Build a set of known table names from the catalog.
    known = {obj["name"] for obj in catalog.get("schema_objects", [])}

    for table in tables:
        if table in known:
            result.tables_resolved.append(table)
        else:
            result.tables_unresolved.append(table)

    if result.tables_unresolved:
        for t in result.tables_unresolved:
            result.warnings.append(
                f"Table '{t}' not found in local schema"
            )
        result.level = "warning"
    else:
        result.level = "info"

    return result


# ---------------------------------------------------------------------------
# Level 3: Column lineage
# ---------------------------------------------------------------------------


def check_lineage(
    sql: str,
    path: Path,
    catalog: dict,
) -> CheckResult:
    """Parse SQL and extract column-level lineage.

    Reports columns that could not be resolved against the catalog.

    Args:
        sql: SQL text (headers stripped).
        path: File path for error reporting.
        catalog: Catalog dict built by ``build_catalog``.

    Returns:
        ``CheckResult`` with lineage info and warnings for unresolved
        columns.
    """
    result = CheckResult(path=path)

    pipeline = (
        inbq.Pipeline()
        .config(raise_exception_on_error=False)
        .parse()
        .extract_lineage(catalog=catalog)
    )
    output = inbq.run_pipeline([sql], pipeline=pipeline)
    ast = output.asts[0]
    lineage = output.lineages[0]

    if isinstance(ast, inbq.PipelineError):
        result.errors.append(ast.error)
        result.level = "error"
        return result

    if isinstance(lineage, inbq.PipelineError):
        result.warnings.append(
            f"Lineage extraction failed: {lineage.error}"
        )
        result.level = "warning"
        return result

    # Extract table references.
    tables = _extract_tables_from_ast(ast)
    result.tables_referenced = tables
    known = {obj["name"] for obj in catalog.get("schema_objects", [])}
    for table in tables:
        if table in known:
            result.tables_resolved.append(table)
        else:
            result.tables_unresolved.append(table)

    if result.tables_unresolved:
        for t in result.tables_unresolved:
            result.info.append(
                f"Table '{t}' not in local schema (cross-project?)"
            )

    # Check referenced columns for unresolved items.
    if hasattr(lineage, "referenced_columns"):
        for ref_obj in lineage.referenced_columns.objects:
            for ref_node in ref_obj.nodes:
                # A column referencing an unknown table is already
                # reported above.  We look for columns whose lineage
                # could not be traced.
                pass  # Referenced columns exist = good

    # Lineage objects: check for nodes with empty inputs which may
    # indicate unresolved columns.
    if hasattr(lineage, "lineage"):
        for lin_obj in lineage.lineage.objects:
            for lin_node in lin_obj.nodes:
                if not lin_node.inputs:
                    # Column has no traced inputs — may be a literal or
                    # an unresolvable reference.
                    result.columns_unresolved.append(
                        f"{lin_obj.name}->{lin_node.name}"
                    )

    if result.columns_unresolved:
        for col in result.columns_unresolved:
            result.warnings.append(
                f"Column '{col}' has no traced lineage inputs"
            )

    # Determine overall level.
    if result.errors:
        result.level = "error"
    elif result.warnings:
        result.level = "warning"
    else:
        result.level = "info"

    return result


# ---------------------------------------------------------------------------
# Orchestrators
# ---------------------------------------------------------------------------


def check_file(
    path: Path,
    output_root: Path,
    project: str,
    catalog: dict | None = None,
) -> CheckResult:
    """Run all analysis levels on a single SQL file.

    Args:
        path: Absolute path to the SQL file.
        output_root: Project-scoped output directory.
        project: GCP project ID.
        catalog: Pre-built catalog (built once for batch).

    Returns:
        Merged ``CheckResult`` for the file.
    """
    raw_sql = path.read_text(encoding="utf-8")

    # JavaScript routines: validate with tree-sitter.
    if _is_js_routine(raw_sql):
        return check_js_routine(raw_sql, path)

    sql = _strip_header(raw_sql)
    if not sql.strip():
        return CheckResult(
            path=path,
            level="info",
            info=["Skipped: empty SQL file"],
        )

    # Level 1: Syntax.
    syntax_result = check_syntax(sql, path)
    if syntax_result.errors:
        return syntax_result

    # Levels 2+3 require a catalog.
    if catalog is None:
        catalog = build_catalog(output_root, project)

    if not catalog.get("schema_objects"):
        # No local schema available — only syntax check.
        syntax_result.info.append(
            "No local schema available; skipping reference and "
            "lineage checks"
        )
        return syntax_result

    # Level 3: Lineage (includes level 2 table reference checking).
    return check_lineage(sql, path, catalog)


def check_files(
    files: list[Path],
    output_root: Path,
    project: str,
) -> CheckSummary:
    """Validate a batch of SQL files.

    Builds the catalog once and reuses it for all files.

    Args:
        files: List of absolute paths to SQL files.
        output_root: Project-scoped output directory.
        project: GCP project ID.

    Returns:
        ``CheckSummary`` with per-file results and counts.
    """
    catalog = build_catalog(output_root, project)
    results: list[CheckResult] = []

    for f in files:
        result = check_file(f, output_root, project, catalog=catalog)
        results.append(result)

    failed = sum(1 for r in results if r.level == "error")
    warned = sum(1 for r in results if r.level == "warning")
    passed = len(results) - failed - warned

    return CheckSummary(
        results=results,
        passed=passed,
        failed=failed,
        warned=warned,
    )


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------


def discover_sql_files(
    output_root: Path,
    include_models: bool = False,
) -> list[Path]:
    """Find all SQL files under the output directory.

    Args:
        output_root: Project-scoped output directory.
        include_models: Whether to include model YAMLs (not SQL).

    Returns:
        Sorted list of SQL file paths.
    """
    sql_files: list[Path] = []
    for sql_dir in _SQL_DIRS:
        for sql_file in output_root.glob(f"*/{sql_dir}/*.sql"):
            sql_files.append(sql_file)
    # Project-level saved queries.
    for sql_file in output_root.glob("saved_queries/*.sql"):
        sql_files.append(sql_file)
    return sorted(set(sql_files))
