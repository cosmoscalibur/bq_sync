//! SQL and JavaScript validation for BigQuery resources (``bq-sync check``).
//!
//! Levels:
//!
//! 1. **Syntax** — parse SQL (``inbq``) / JavaScript (``tree-sitter``).
//! 2. **References** — referenced tables vs. the local model catalog.
//! 3. **Lineage** — column-level lineage; flags columns without inputs.
//! 4. **Contract** (JS) — return statement / argument count vs. model YAML.
//! 5. **Dry-run** (JS, ``--online``) — validation by the BigQuery API.

use std::collections::{BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use inbq::lineage::catalog::{Catalog, Column, SchemaObject, SchemaObjectKind};
use regex::Regex;
use serde_json::Value;

use crate::api::BqApi;
use crate::utils::{ends_yaml_block, py_repr, split_lines, yaml_entry_parts};

static ROUTINE_HEADER_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?m)^--\s*Routine:\s*.+$\n--\s*Language:\s*(.+)$").unwrap());

const SQL_DIRS: [&str; 3] = ["views", "routines", "saved_queries"];

/// Severity of a [`CheckResult`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum Level {
    #[default]
    Info,
    Warning,
    Error,
}

impl Level {
    pub fn as_str(self) -> &'static str {
        match self {
            Level::Info => "info",
            Level::Warning => "warning",
            Level::Error => "error",
        }
    }
}

/// Validation result for a single file.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CheckResult {
    pub path: PathBuf,
    pub level: Level,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub info: Vec<String>,
    pub tables_referenced: Vec<String>,
    pub tables_resolved: Vec<String>,
    pub tables_unresolved: Vec<String>,
    pub columns_unresolved: Vec<String>,
}

impl CheckResult {
    pub fn new(path: &Path) -> Self {
        CheckResult {
            path: path.to_path_buf(),
            ..Default::default()
        }
    }

    fn with_info(path: &Path, msg: &str) -> Self {
        CheckResult {
            info: vec![msg.to_owned()],
            ..CheckResult::new(path)
        }
    }

    /// Recompute ``level`` from errors / warnings.
    fn settle(mut self) -> Self {
        self.level = if !self.errors.is_empty() {
            Level::Error
        } else if !self.warnings.is_empty() {
            Level::Warning
        } else {
            Level::Info
        };
        self
    }
}

/// Aggregate results; ``passed``, ``failed`` and ``warned`` are disjoint.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CheckSummary {
    pub results: Vec<CheckResult>,
    pub passed: usize,
    pub failed: usize,
    pub warned: usize,
}

// ---------------------------------------------------------------------------
// Catalog
// ---------------------------------------------------------------------------

/// Map a BigQuery type name to an ``inbq`` dtype.
pub fn map_bq_type(bq_type: &str) -> String {
    let upper = bq_type.trim().to_uppercase();
    let scalar = |t: &str| -> Option<&'static str> {
        Some(match t {
            "STRING" => "string",
            "BYTES" => "bytes",
            "INTEGER" | "INT64" => "int64",
            "FLOAT" | "FLOAT64" => "float64",
            "NUMERIC" => "numeric",
            "BIGNUMERIC" => "bignumeric",
            "BOOLEAN" | "BOOL" => "bool",
            "TIMESTAMP" => "timestamp",
            "DATE" => "date",
            "TIME" => "time",
            "DATETIME" => "datetime",
            "GEOGRAPHY" => "geography",
            "JSON" => "json",
            "RECORD" | "STRUCT" => "struct",
            _ => return None,
        })
    };
    if let Some(t) = scalar(&upper) {
        return t.to_owned();
    }
    for base in ["ARRAY", "STRUCT"] {
        if let Some(rest) = upper.strip_prefix(base)
            && !rest.starts_with(|c: char| c.is_alphanumeric() || c == '_')
        {
            return scalar(base).unwrap_or("array").to_owned();
        }
    }
    bq_type.to_lowercase()
}

/// Schema fields (``(name, type)``) of a model YAML.
pub fn parse_yaml_schema(text: &str) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    let mut in_schema = false;
    for line in split_lines(text) {
        let stripped = line.trim();
        if stripped == "schema:" {
            in_schema = true;
            continue;
        }
        if !in_schema {
            continue;
        }
        if ends_yaml_block(line) {
            break;
        }
        if stripped.starts_with("- name:") {
            let (name, dtype) = name_and_type(stripped);
            if !name.is_empty() {
                fields.push((name, dtype));
            }
        }
    }
    fields
}

fn name_and_type(stripped: &str) -> (String, String) {
    let mut name = String::new();
    let mut dtype = String::new();
    for part in yaml_entry_parts(stripped) {
        if let Some(v) = part.strip_prefix("name:") {
            name = v.trim().to_owned();
        } else if let Some(v) = part.strip_prefix("type:") {
            dtype = v.trim().to_owned();
        }
    }
    (name, dtype)
}

/// The first ``name:`` value of a model YAML.
pub fn parse_yaml_name(text: &str) -> String {
    split_lines(text)
        .iter()
        .find_map(|l| l.trim().strip_prefix("name:").map(|v| v.trim().to_owned()))
        .unwrap_or_default()
}

fn glob_yaml(output_root: &Path, sub: &str) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(datasets) = std::fs::read_dir(output_root) else {
        return out;
    };
    for ds in datasets.flatten() {
        let Ok(entries) = std::fs::read_dir(ds.path().join(sub)) else {
            continue;
        };
        out.extend(
            entries
                .flatten()
                .map(|e| e.path())
                .filter(|p| p.is_file() && p.extension().is_some_and(|e| e == "yaml")),
        );
    }
    out.sort();
    out
}

/// Build an ``inbq`` catalog from ``<dataset>/models/*.yaml`` and
/// ``<dataset>/externals/*.yaml`` under *output_root*.
pub fn build_catalog(output_root: &Path, project: &str) -> Catalog {
    let mut schema_objects = Vec::new();
    for sub in ["models", "externals"] {
        for path in glob_yaml(output_root, sub) {
            let Ok(text) = std::fs::read_to_string(&path) else {
                continue;
            };
            let fields = parse_yaml_schema(&text);
            if fields.is_empty() {
                continue;
            }
            let table = parse_yaml_name(&text);
            if table.is_empty() {
                continue;
            }
            let dataset = path
                .parent()
                .and_then(Path::parent)
                .and_then(Path::file_name)
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            let columns = fields
                .into_iter()
                .map(|(name, dtype)| Column {
                    name,
                    dtype: map_bq_type(if dtype.is_empty() { "STRING" } else { &dtype }),
                })
                .collect();
            schema_objects.push(SchemaObject {
                name: format!("{project}.{dataset}.{table}"),
                kind: SchemaObjectKind::Table { columns },
            });
        }
    }
    log::debug!(
        "Built catalog with {} schema objects from {}",
        schema_objects.len(),
        output_root.display()
    );
    Catalog { schema_objects }
}

// ---------------------------------------------------------------------------
// Pre-processing helpers
// ---------------------------------------------------------------------------

/// Strip all leading ``--`` comment and blank lines.
pub fn strip_header(sql: &str) -> String {
    let lines = split_lines(sql);
    let start = lines
        .iter()
        .position(|l| {
            let s = l.trim();
            !(s.starts_with("--") || s.is_empty())
        })
        .unwrap_or(lines.len());
    lines[start..].join("\n")
}

/// Whether the routine header declares a JavaScript routine.
pub fn is_js_routine(sql: &str) -> bool {
    ROUTINE_HEADER_RE
        .captures(sql)
        .is_some_and(|m| matches!(m[1].trim().to_uppercase().as_str(), "JAVASCRIPT" | "JS"))
}

/// Routine model YAML for a routine SQL file:
/// ``<dataset>/routines/models/<name>.yaml``.
pub fn find_routine_model_yaml(sql_path: &Path) -> Option<PathBuf> {
    let stem = sql_path.file_stem()?;
    let yaml = sql_path
        .parent()?
        .join("models")
        .join(format!("{}.yaml", stem.to_string_lossy()));
    yaml.is_file().then_some(yaml)
}

/// Return type and ``(name, type)`` arguments of a routine model YAML.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RoutineModel {
    pub return_type: Option<String>,
    pub arguments: Vec<(String, String)>,
}

pub fn parse_routine_model_yaml(text: &str) -> RoutineModel {
    let mut model = RoutineModel::default();
    let mut in_arguments = false;
    for line in split_lines(text) {
        let stripped = line.trim();
        if let Some(v) = stripped.strip_prefix("return_type:") {
            model.return_type = Some(v.trim().to_owned());
            continue;
        }
        if stripped == "arguments:" {
            in_arguments = true;
            continue;
        }
        if in_arguments {
            if ends_yaml_block(line) {
                break;
            }
            if stripped.starts_with("- name:") {
                let (name, dtype) = name_and_type(stripped);
                if !name.is_empty() {
                    model.arguments.push((name, dtype));
                }
            }
        }
    }
    model
}

// ---------------------------------------------------------------------------
// JavaScript (tree-sitter)
// ---------------------------------------------------------------------------

fn parse_js(js_body: &str) -> Option<tree_sitter::Tree> {
    let mut parser = tree_sitter::Parser::new();
    parser
        .set_language(&tree_sitter_javascript::LANGUAGE.into())
        .ok()?;
    parser.parse(js_body, None)
}

/// Pre-order traversal of every node (named and anonymous).
fn walk(node: tree_sitter::Node<'_>) -> Vec<tree_sitter::Node<'_>> {
    let mut out = Vec::new();
    let mut stack = vec![node];
    while let Some(n) = stack.pop() {
        out.push(n);
        let mut cursor = n.walk();
        let children: Vec<_> = n.children(&mut cursor).collect();
        stack.extend(children.into_iter().rev());
    }
    out
}

/// Validate JavaScript syntax (``ERROR`` / missing nodes).
pub fn check_js_syntax(js_body: &str, path: &Path) -> CheckResult {
    let mut result = CheckResult::new(path);
    let Some(tree) = parse_js(js_body) else {
        result
            .info
            .push("tree-sitter-javascript not available; JS syntax check skipped".into());
        return result;
    };
    for node in walk(tree.root_node()) {
        if !(node.is_error() || node.is_missing()) {
            continue;
        }
        let pos = node.start_position();
        let (row, col) = (pos.row + 1, pos.column + 1);
        if node.is_missing() {
            result.errors.push(format!(
                "[line {row}, col {col}] Missing expected token: {}",
                node.kind()
            ));
        } else {
            let text = node.utf8_text(js_body.as_bytes()).unwrap_or_default();
            let snippet: String = text.chars().take(40).collect();
            result.errors.push(format!(
                "[line {row}, col {col}] Syntax error near: {}",
                py_repr(&snippet)
            ));
        }
    }
    result.settle()
}

/// Check the JS body against the routine model YAML (return / arity).
pub fn check_js_contract(
    js_body: &str,
    path: &Path,
    model_yaml_path: Option<&Path>,
) -> CheckResult {
    let mut result = CheckResult::new(path);
    let Some(tree) = parse_js(js_body) else {
        return result;
    };
    let nodes = walk(tree.root_node());
    let has_return = nodes.iter().any(|n| n.kind() == "return_statement");

    let model = model_yaml_path
        .filter(|p| p.is_file())
        .and_then(|p| std::fs::read_to_string(p).ok())
        .map(|t| parse_routine_model_yaml(&t));

    if let Some(model) = model {
        if let Some(rt) = model.return_type.as_deref().filter(|r| !r.is_empty())
            && !has_return
        {
            result.warnings.push(format!(
                "Routine declares return_type '{rt}' but JS body has no return statement"
            ));
        }
        // Heuristic: BQ passes JS UDF args as globals, so a wrapper function
        // is optional; only compare when one is declared.
        if let Some(func) = nodes.iter().find(|n| n.kind() == "function_declaration") {
            let params = walk(*func)
                .into_iter()
                .find(|n| n.kind() == "formal_parameters");
            if let Some(params) = params {
                let mut cursor = params.walk();
                let count = params
                    .children(&mut cursor)
                    .filter(|c| c.kind() == "identifier")
                    .count();
                if count != model.arguments.len() {
                    result.warnings.push(format!(
                        "YAML declares {} argument(s) but JS function has {count} parameter(s)",
                        model.arguments.len()
                    ));
                }
            }
        }
    }
    result.settle()
}

/// ``CREATE TEMP FUNCTION`` + invocation used for dry-run validation.
pub fn build_js_udf_query(
    name: &str,
    body: &str,
    arguments: &[(String, String)],
    return_type: &str,
) -> String {
    let args_sql = arguments
        .iter()
        .map(|(n, t)| format!("{n} {t}"))
        .collect::<Vec<_>>()
        .join(", ");
    let null_args = vec!["NULL"; arguments.len()].join(", ");
    format!(
        "CREATE TEMP FUNCTION `{name}`({args_sql})\n\
         RETURNS {return_type}\n\
         LANGUAGE js\n\
         AS r\"\"\"\n{body}\n\"\"\";\n\
         SELECT `{name}`({null_args});\n"
    )
}

/// Validate a JS routine with a BigQuery dry run.
pub fn check_js_dryrun(
    js_body: &str,
    path: &Path,
    project: &str,
    model_yaml_path: Option<&Path>,
    api: &dyn BqApi,
) -> CheckResult {
    let Some(text) = model_yaml_path
        .filter(|p| p.is_file())
        .and_then(|p| std::fs::read_to_string(p).ok())
    else {
        return CheckResult::with_info(
            path,
            "Dry-run skipped: no model YAML available for routine reconstruction",
        );
    };
    let model = parse_routine_model_yaml(&text);
    let name = parse_yaml_name(&text);
    if name.is_empty() {
        return CheckResult::with_info(path, "Dry-run skipped: could not parse routine name");
    }
    let Some(return_type) = model.return_type.filter(|r| !r.is_empty()) else {
        return CheckResult::with_info(path, "Dry-run skipped: no return_type in model YAML");
    };
    let query = build_js_udf_query(&name, js_body, &model.arguments, &return_type);
    match api.dry_run_query(project, &query) {
        Ok(()) => CheckResult::with_info(path, "Dry-run: OK"),
        Err(e) => {
            let mut r = CheckResult::new(path);
            r.errors.push(format!("Dry-run failed: {e}"));
            r.settle()
        }
    }
}

/// Run all JS levels on a routine file (raw content, header included).
pub fn check_js_routine(
    raw_sql: &str,
    path: &Path,
    online: Option<(&dyn BqApi, &str)>,
) -> CheckResult {
    let js_body = strip_header(raw_sql);
    if js_body.trim().is_empty() {
        return CheckResult::with_info(path, "Skipped: empty JS routine body");
    }
    let syntax = check_js_syntax(&js_body, path);
    if !syntax.errors.is_empty() {
        return syntax;
    }
    let model_path = find_routine_model_yaml(path);
    let contract = check_js_contract(&js_body, path, model_path.as_deref());

    let mut result = CheckResult::new(path);
    result.info.extend(syntax.info);
    result.warnings.extend(contract.warnings);
    result.info.extend(contract.info);

    if let Some((api, project)) = online {
        let dry = check_js_dryrun(&js_body, path, project, model_path.as_deref(), api);
        result.errors.extend(dry.errors);
        result.warnings.extend(dry.warnings);
        result.info.extend(dry.info);
    }
    result.settle()
}

// ---------------------------------------------------------------------------
// SQL (inbq)
// ---------------------------------------------------------------------------

fn parse_sql(sql: &str) -> Result<inbq::ast::Ast, String> {
    inbq::parser::parse_sql(sql).map_err(|e| format!("{e:#}"))
}

/// Level 1: syntax.
pub fn check_syntax(sql: &str, path: &Path) -> CheckResult {
    let mut result = CheckResult::new(path);
    if let Err(e) = parse_sql(sql) {
        result.errors.push(e);
    }
    result.settle()
}

/// Table names in ``FROM`` / ``JOIN`` clauses (sorted, deduplicated).
pub fn extract_tables(ast: &inbq::ast::Ast) -> Vec<String> {
    fn visit(v: &Value, out: &mut BTreeSet<String>) {
        match v {
            Value::Object(map) => {
                // `FromExpr::Path(FromPathExpr { path: PathName { name, .. }, .. })`
                if let Some(name) = map
                    .get("Path")
                    .and_then(|p| p.get("path"))
                    .and_then(|p| p.get("name"))
                    .and_then(Value::as_str)
                    .filter(|n| !n.is_empty())
                {
                    out.insert(name.to_owned());
                }
                map.values().for_each(|c| visit(c, out));
            }
            Value::Array(items) => items.iter().for_each(|c| visit(c, out)),
            _ => {}
        }
    }
    let mut out = BTreeSet::new();
    if let Ok(json) = serde_json::to_value(ast) {
        visit(&json, &mut out);
    }
    out.into_iter().collect()
}

fn resolve_tables(result: &mut CheckResult, tables: Vec<String>, catalog: &Catalog) {
    let known: HashSet<&str> = catalog
        .schema_objects
        .iter()
        .map(|o| o.name.as_str())
        .collect();
    for t in &tables {
        if known.contains(t.as_str()) {
            result.tables_resolved.push(t.clone());
        } else {
            result.tables_unresolved.push(t.clone());
        }
    }
    result.tables_referenced = tables;
}

/// Level 2: table references against the catalog.
pub fn check_references(sql: &str, path: &Path, catalog: &Catalog) -> CheckResult {
    let mut result = CheckResult::new(path);
    let ast = match parse_sql(sql) {
        Ok(ast) => ast,
        Err(e) => {
            result.errors.push(e);
            return result.settle();
        }
    };
    resolve_tables(&mut result, extract_tables(&ast), catalog);
    for t in &result.tables_unresolved {
        result
            .warnings
            .push(format!("Table '{t}' not found in local schema"));
    }
    result.settle()
}

/// Level 3: column lineage (includes level 2 reference resolution).
pub fn check_lineage(sql: &str, path: &Path, catalog: &Catalog) -> CheckResult {
    let mut result = CheckResult::new(path);
    let ast = match parse_sql(sql) {
        Ok(ast) => ast,
        Err(e) => {
            result.errors.push(e);
            return result.settle();
        }
    };
    let lineage = inbq::lineage::extract_lineage(&[&ast], catalog, false, false)
        .pop()
        .unwrap_or_else(|| Err(anyhow::anyhow!("no lineage produced")));
    let lineage = match lineage {
        Ok(l) => l,
        Err(e) => {
            result
                .warnings
                .push(format!("Lineage extraction failed: {e:#}"));
            return result.settle();
        }
    };

    resolve_tables(&mut result, extract_tables(&ast), catalog);
    for t in &result.tables_unresolved {
        result
            .info
            .push(format!("Table '{t}' not in local schema (cross-project?)"));
    }
    for obj in &lineage.lineage.objects {
        for node in obj.nodes.iter().filter(|n| n.inputs.is_empty()) {
            result
                .columns_unresolved
                .push(format!("{}->{}", obj.name, node.name));
        }
    }
    for col in &result.columns_unresolved {
        result
            .warnings
            .push(format!("Column '{col}' has no traced lineage inputs"));
    }
    result.settle()
}

// ---------------------------------------------------------------------------
// Orchestrators
// ---------------------------------------------------------------------------

/// Run every applicable level on one file.
///
/// *online* enables the BigQuery dry run for JS routines.
pub fn check_file(
    path: &Path,
    output_root: &Path,
    project: &str,
    catalog: Option<&Catalog>,
    online: Option<&dyn BqApi>,
) -> CheckResult {
    let raw = match std::fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(e) => {
            let mut r = CheckResult::new(path);
            r.errors.push(format!("Cannot read file: {e}"));
            return r.settle();
        }
    };
    if is_js_routine(&raw) {
        return check_js_routine(&raw, path, online.map(|api| (api, project)));
    }
    let sql = strip_header(&raw);
    if sql.trim().is_empty() {
        return CheckResult::with_info(path, "Skipped: empty SQL file");
    }
    let mut syntax = check_syntax(&sql, path);
    if !syntax.errors.is_empty() {
        return syntax;
    }
    let owned;
    let catalog = match catalog {
        Some(c) => c,
        None => {
            owned = build_catalog(output_root, project);
            &owned
        }
    };
    if catalog.schema_objects.is_empty() {
        syntax
            .info
            .push("No local schema available; skipping reference and lineage checks".into());
        return syntax;
    }
    check_lineage(&sql, path, catalog)
}

/// Validate a batch of files (catalog built once).
pub fn check_files(
    files: &[PathBuf],
    output_root: &Path,
    project: &str,
    online: Option<&dyn BqApi>,
) -> CheckSummary {
    let catalog = build_catalog(output_root, project);
    let results: Vec<CheckResult> = files
        .iter()
        .map(|f| check_file(f, output_root, project, Some(&catalog), online))
        .collect();
    let failed = results.iter().filter(|r| r.level == Level::Error).count();
    let warned = results.iter().filter(|r| r.level == Level::Warning).count();
    CheckSummary {
        passed: results.len() - failed - warned,
        failed,
        warned,
        results,
    }
}

/// All SQL files of the output tree (sorted).
pub fn discover_sql_files(output_root: &Path) -> Vec<PathBuf> {
    let mut found = BTreeSet::new();
    let sql_in = |dir: &Path, found: &mut BTreeSet<PathBuf>| {
        if let Ok(entries) = std::fs::read_dir(dir) {
            found.extend(
                entries
                    .flatten()
                    .map(|e| e.path())
                    .filter(|p| p.is_file() && p.extension().is_some_and(|e| e == "sql")),
            );
        }
    };
    if let Ok(entries) = std::fs::read_dir(output_root) {
        for entry in entries.flatten() {
            for sub in SQL_DIRS {
                sql_in(&entry.path().join(sub), &mut found);
            }
        }
    }
    sql_in(&output_root.join("saved_queries"), &mut found);
    found.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::FakeApi;

    fn w(p: &Path, s: &str) {
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, s).unwrap();
    }

    /// Mirrors the Python test fixture project.
    fn project() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("my_project");
        let ds = root.join("my_dataset");
        w(
            &ds.join("models/users.yaml"),
            "name: users\ndescription: \"User table\"\nschema:\n  - name: id  type: INTEGER  mode: REQUIRED  description: \"PK\"\n  - name: email  type: STRING  mode: NULLABLE  description: \"\"\n  - name: active  type: BOOLEAN  mode: NULLABLE  description: \"\"\n",
        );
        w(
            &ds.join("models/orders.yaml"),
            "name: orders\ndescription: \"Order table\"\nschema:\n  - name: order_id  type: INTEGER  mode: REQUIRED  description: \"\"\n  - name: user_id  type: INTEGER  mode: NULLABLE  description: \"\"\n  - name: total  type: FLOAT64  mode: NULLABLE  description: \"\"\n",
        );
        w(
            &ds.join("views/active_users.sql"),
            "SELECT id, email FROM `my_project.my_dataset.users` WHERE active = TRUE\n",
        );
        w(&ds.join("views/broken.sql"), "SELECT FROM WHERE\n");
        w(
            &ds.join("routines/calc.sql"),
            "-- Routine: calc\n-- Language: SQL\nDECLARE x INT64 DEFAULT 0;\nINSERT INTO `my_project.my_dataset.orders`\nSELECT 1 AS order_id, 42 AS user_id, 99.9 AS total;\n",
        );
        w(
            &ds.join("routines/transform.sql"),
            "-- Routine: transform\n-- Language: JAVASCRIPT\nfunction transform(x) {\n  return x * 2;\n}\n",
        );
        w(
            &ds.join("routines/broken_js.sql"),
            "-- Routine: broken_js\n-- Language: JAVASCRIPT\nfunction bad(x { return x; }\n",
        );
        w(
            &ds.join("routines/models/transform.yaml"),
            "name: transform\ndescription: \"Transforms input\"\nlanguage: JAVASCRIPT\nreturn_type: FLOAT64\narguments:\n  - name: x  type: FLOAT64  mode: IN\n",
        );
        w(
            &root.join("saved_queries/monthly.sql"),
            "-- Saved Query: monthly\nSELECT order_id, total\nFROM `my_project.my_dataset.orders`\nWHERE total > 100\n",
        );
        (dir, root)
    }

    #[test]
    fn helpers() {
        assert_eq!(
            strip_header("-- Routine: foo\n-- Language: SQL\n\nSELECT 1"),
            "SELECT 1"
        );
        assert_eq!(strip_header("-- Saved Query: bar\n\nSELECT 2"), "SELECT 2");
        assert_eq!(
            strip_header("SELECT 1\n-- comment inside"),
            "SELECT 1\n-- comment inside"
        );
        assert_eq!(strip_header(""), "");
        assert_eq!(strip_header("-- only\n-- more").trim(), "");

        assert!(is_js_routine(
            "-- Routine: foo\n-- Language: JAVASCRIPT\n\nreturn 1;"
        ));
        assert!(is_js_routine(
            "-- Routine: foo\n-- Language: JS\n\nreturn 1;"
        ));
        assert!(!is_js_routine(
            "-- Routine: foo\n-- Language: SQL\n\nSELECT 1"
        ));
        assert!(!is_js_routine("SELECT 1"));

        assert_eq!(
            parse_yaml_schema(
                "name: test\nschema:\n  - name: id  type: INTEGER  mode: REQUIRED  description: \"\"\n  - name: val  type: FLOAT64  mode: NULLABLE  description: \"\"\n"
            ),
            vec![
                ("id".into(), "INTEGER".into()),
                ("val".into(), "FLOAT64".into())
            ]
        );
        assert!(parse_yaml_schema("name: test\n").is_empty());
        assert_eq!(
            parse_yaml_name("name: my_table\ndescription: foo\n"),
            "my_table"
        );
        assert_eq!(parse_yaml_name("description: foo\n"), "");
    }

    #[test]
    fn bq_type_mapping() {
        assert_eq!(map_bq_type("STRING"), "string");
        assert_eq!(map_bq_type("INT64"), "int64");
        assert_eq!(map_bq_type("FLOAT64"), "float64");
        assert_eq!(map_bq_type("BOOL"), "bool");
        assert_eq!(map_bq_type("string"), "string");
        assert_eq!(map_bq_type("Int64"), "int64");
        assert_eq!(map_bq_type("ARRAY<STRING>"), "array");
        assert_eq!(map_bq_type("STRUCT<x INT64, y STRING>"), "struct");
        assert_eq!(map_bq_type("ARRAY<STRUCT<a INT64>>"), "array");
        assert_eq!(map_bq_type("INTERVAL"), "interval");
    }

    #[test]
    fn catalog() {
        let (_d, root) = project();
        let cat = build_catalog(&root, "my_project");
        let names: HashSet<_> = cat.schema_objects.iter().map(|o| o.name.as_str()).collect();
        assert_eq!(
            names,
            HashSet::from([
                "my_project.my_dataset.users",
                "my_project.my_dataset.orders"
            ])
        );
        let users = cat
            .schema_objects
            .iter()
            .find(|o| o.name.ends_with(".users"))
            .unwrap();
        let SchemaObjectKind::Table { columns } = &users.kind else {
            panic!("expected table")
        };
        let dtypes: Vec<_> = columns
            .iter()
            .map(|c| (c.name.as_str(), c.dtype.as_str()))
            .collect();
        assert_eq!(
            dtypes,
            vec![("id", "int64"), ("email", "string"), ("active", "bool")]
        );

        let empty = tempfile::tempdir().unwrap();
        assert!(build_catalog(empty.path(), "p").schema_objects.is_empty());
    }

    #[test]
    fn syntax_level() {
        let p = Path::new("t.sql");
        assert_eq!(check_syntax("SELECT 1 AS x", p).level, Level::Info);
        let bad = check_syntax("SELECT FROM WHERE", p);
        assert_eq!(bad.level, Level::Error);
        assert!(!bad.errors.is_empty());
        assert_ne!(
            check_syntax(
                "DECLARE x INT64 DEFAULT 0;\nINSERT INTO `p.d.t` SELECT 1 AS id;",
                p
            )
            .level,
            Level::Error
        );
        assert_ne!(
            check_syntax("SELECT 1; SELECT 2; SELECT 3;", p).level,
            Level::Error
        );
    }

    #[test]
    fn reference_and_lineage_levels() {
        let (_d, root) = project();
        let cat = build_catalog(&root, "my_project");
        let p = Path::new("t.sql");

        let r = check_references("SELECT id FROM `my_project.my_dataset.users`", p, &cat);
        assert_eq!(r.tables_resolved, vec!["my_project.my_dataset.users"]);
        assert!(r.tables_unresolved.is_empty());

        let r = check_references("SELECT id FROM `other_project.other.table`", p, &cat);
        assert_eq!(r.tables_unresolved, vec!["other_project.other.table"]);
        assert_eq!(r.level, Level::Warning);
        assert_eq!(
            check_references("SELECT FROM WHERE", p, &cat).level,
            Level::Error
        );

        let r = check_lineage(
            "SELECT id, email FROM `my_project.my_dataset.users`",
            p,
            &cat,
        );
        assert_ne!(r.level, Level::Error);
        assert!(
            r.tables_resolved
                .contains(&"my_project.my_dataset.users".to_owned())
        );
        assert_ne!(
            check_lineage("SELECT x FROM `unknown.ds.tbl`", p, &cat).level,
            Level::Error
        );
        assert_eq!(
            check_lineage("SELECT FROM WHERE", p, &cat).level,
            Level::Error
        );
    }

    #[test]
    fn check_file_cases() {
        let (_d, root) = project();
        let ds = root.join("my_dataset");
        let check = |rel: &str| check_file(&root.join(rel), &root, "my_project", None, None);
        assert_ne!(
            check("my_dataset/views/active_users.sql").level,
            Level::Error
        );
        let broken = check("my_dataset/views/broken.sql");
        assert_eq!(broken.level, Level::Error);
        assert!(!broken.errors.is_empty());
        assert_ne!(
            check("my_dataset/routines/transform.sql").level,
            Level::Error
        );
        assert_eq!(
            check("my_dataset/routines/broken_js.sql").level,
            Level::Error
        );
        assert_ne!(check("my_dataset/routines/calc.sql").level, Level::Error);
        assert_ne!(check("saved_queries/monthly.sql").level, Level::Error);
        assert!(ds.exists());
    }

    #[test]
    fn batch_and_discovery() {
        let (_d, root) = project();
        let files = vec![
            root.join("my_dataset/views/active_users.sql"),
            root.join("my_dataset/views/broken.sql"),
        ];
        let s = check_files(&files, &root, "my_project", None);
        assert!(s.failed >= 1 && s.passed >= 1);
        assert_eq!(s.passed + s.failed + s.warned, files.len());

        let s = check_files(&[], &root, "my_project", None);
        assert_eq!((s.passed, s.failed, s.warned), (0, 0, 0));

        let all = discover_sql_files(&root);
        let names: HashSet<_> = all
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        for n in [
            "active_users.sql",
            "broken.sql",
            "calc.sql",
            "transform.sql",
            "broken_js.sql",
            "monthly.sql",
        ] {
            assert!(names.contains(n), "{n}");
        }
        let s = check_files(&all, &root, "my_project", None);
        assert_eq!(s.passed + s.failed + s.warned, all.len());

        let empty = tempfile::tempdir().unwrap();
        assert!(discover_sql_files(empty.path()).is_empty());
    }

    #[test]
    fn routine_model_parsing() {
        let m = parse_routine_model_yaml(
            "name: transform\nlanguage: JAVASCRIPT\nreturn_type: FLOAT64\narguments:\n  - name: x  type: FLOAT64  mode: IN\n  - name: y  type: STRING  mode: IN\n",
        );
        assert_eq!(m.return_type.as_deref(), Some("FLOAT64"));
        assert_eq!(
            m.arguments,
            vec![
                ("x".into(), "FLOAT64".into()),
                ("y".into(), "STRING".into())
            ]
        );
        let m = parse_routine_model_yaml(
            "name: proc\nlanguage: JAVASCRIPT\narguments:\n  - name: x  type: INT64  mode: IN\n",
        );
        assert_eq!((m.return_type, m.arguments.len()), (None, 1));
        assert_eq!(parse_routine_model_yaml(""), RoutineModel::default());

        let (_d, root) = project();
        let routines = root.join("my_dataset/routines");
        assert!(find_routine_model_yaml(&routines.join("transform.sql")).is_some());
        assert!(find_routine_model_yaml(&routines.join("nonexistent.sql")).is_none());
    }

    #[test]
    fn js_syntax() {
        let p = Path::new("t.sql");
        for ok in [
            "function f(x, y) { return x * y; }",
            "return x + y;",
            "const double = (x) => x * 2;",
            "function transform(x) {\n  var result = x * 2;\n  if (result > 100) {\n    result = 100;\n  }\n  return result;\n}\n",
            "",
        ] {
            assert_ne!(check_js_syntax(ok, p).level, Level::Error, "{ok}");
        }
        for bad in ["function f(x { return x; }", "function f() { return +++; }"] {
            let r = check_js_syntax(bad, p);
            assert_eq!(r.level, Level::Error, "{bad}");
            assert!(!r.errors.is_empty());
        }
        let r = check_js_syntax("function f(x {\n  return x;\n}", p);
        assert!(r.errors.iter().any(|e| e.contains("[line")));
    }

    #[test]
    fn js_contract() {
        let dir = tempfile::tempdir().unwrap();
        let y = dir.path().join("model.yaml");
        let p = Path::new("t.sql");
        let contract = |js: &str, yaml: &str| {
            std::fs::write(&y, yaml).unwrap();
            check_js_contract(js, p, Some(&y))
        };

        let r = contract(
            "function transform(x) { return x * 2; }",
            "name: transform\nreturn_type: FLOAT64\narguments:\n  - name: x  type: FLOAT64  mode: IN\n",
        );
        assert!(r.warnings.is_empty());

        let r = contract("var x = 42;", "name: test\nreturn_type: FLOAT64\n");
        assert_eq!(r.level, Level::Warning);
        assert!(
            r.warnings
                .iter()
                .any(|w| w.to_lowercase().contains("return"))
        );

        let r = contract(
            "function f(a, b, c) { return a + b + c; }",
            "name: f\nreturn_type: FLOAT64\narguments:\n  - name: a  type: FLOAT64  mode: IN\n  - name: b  type: FLOAT64  mode: IN\n",
        );
        assert!(
            r.warnings
                .iter()
                .any(|w| w.to_lowercase().contains("argument"))
        );

        let r = contract(
            "return x + y;",
            "name: add\nreturn_type: FLOAT64\narguments:\n  - name: x  type: FLOAT64  mode: IN\n  - name: y  type: FLOAT64  mode: IN\n",
        );
        assert!(
            !r.warnings
                .iter()
                .any(|w| w.to_lowercase().contains("argument"))
        );

        let r = contract(
            "console.log('hello');",
            "name: side_effect\nlanguage: JAVASCRIPT\n",
        );
        assert!(r.warnings.is_empty());

        assert_ne!(
            check_js_contract("function f(x) { return x; }", p, None).level,
            Level::Warning
        );
    }

    #[test]
    fn js_routine_and_dryrun() {
        let (_d, root) = project();
        let path = root.join("my_dataset/routines/transform.sql");
        let raw = std::fs::read_to_string(&path).unwrap();
        assert_ne!(check_js_routine(&raw, &path, None).level, Level::Error);

        let broken = root.join("my_dataset/routines/broken_js.sql");
        let raw_b = std::fs::read_to_string(&broken).unwrap();
        assert_eq!(check_js_routine(&raw_b, &broken, None).level, Level::Error);

        let r = check_js_routine(
            "-- Routine: empty\n-- Language: JAVASCRIPT\n\n",
            Path::new("e.sql"),
            None,
        );
        assert_eq!(r.level, Level::Info);
        assert!(r.info.iter().any(|m| m.to_lowercase().contains("empty")));

        // Online: the dry run is executed and its result merged.
        let api = FakeApi::default();
        let r = check_js_routine(&raw, &path, Some((&api, "my_project")));
        assert!(r.info.iter().any(|m| m == "Dry-run: OK"));
        assert!(
            api.calls()[0].starts_with(
                "dry_run_query my_project CREATE TEMP FUNCTION `transform`(x FLOAT64)"
            )
        );

        *api.dry_run_error.borrow_mut() = Some("Syntax error in JavaScript UDF".into());
        let r = check_js_routine(&raw, &path, Some((&api, "my_project")));
        assert_eq!(r.level, Level::Error);
        assert!(
            r.errors
                .iter()
                .any(|e| e.to_lowercase().contains("dry-run"))
        );
    }

    #[test]
    fn dryrun_skips() {
        let dir = tempfile::tempdir().unwrap();
        let api = FakeApi::default();
        let p = Path::new("t.sql");
        let r = check_js_dryrun("return 1;", p, "proj", None, &api);
        assert!(r.info.iter().any(|m| m.to_lowercase().contains("skipped")));
        let y = dir.path().join("model.yaml");
        std::fs::write(&y, "name: proc\nlanguage: JAVASCRIPT\n").unwrap();
        let r = check_js_dryrun("console.log(1);", p, "proj", Some(&y), &api);
        assert!(r.info.iter().any(|m| m.contains("return_type")));
        assert!(api.calls().is_empty());
    }

    #[test]
    fn udf_query() {
        let q = build_js_udf_query(
            "double",
            "return x * 2;",
            &[("x".into(), "FLOAT64".into())],
            "FLOAT64",
        );
        for s in [
            "CREATE TEMP FUNCTION `double`",
            "x FLOAT64",
            "RETURNS FLOAT64",
            "LANGUAGE js",
            "return x * 2;",
            "SELECT `double`(NULL)",
        ] {
            assert!(q.contains(s), "{s}");
        }
        let q = build_js_udf_query(
            "add",
            "return a + b;",
            &[
                ("a".into(), "FLOAT64".into()),
                ("b".into(), "FLOAT64".into()),
            ],
            "FLOAT64",
        );
        assert!(q.contains("a FLOAT64, b FLOAT64") && q.contains("SELECT `add`(NULL, NULL)"));
        let q = build_js_udf_query("hello", "return \"hello\";", &[], "STRING");
        assert!(q.contains("SELECT `hello`()"));
    }
}
