//! Read local files (produced by ``pull``) into push update descriptors.

use std::collections::HashMap;
use std::path::Path;
use std::sync::LazyLock;

use regex::Regex;

use crate::error::{Error, Result};
use crate::resources::RoutineArgument;
use crate::utils::{
    decode_json_str, ends_yaml_block, split_lines, split_lines_keepends, stem, yaml_entry_parts,
};

/// Mutable fields for a view update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewUpdate {
    pub name: String,
    pub sql: String,
}

/// Mutable description fields for a table / view model.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ModelUpdate {
    pub name: String,
    pub description: String,
    pub field_descriptions: HashMap<String, String>,
}

/// Mutable fields for a saved query update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SavedQueryUpdate {
    pub name: String,
    pub sql: String,
}

/// Mutable fields for a routine update (SQL or JavaScript).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutineUpdate {
    pub name: String,
    pub body: String,
    pub language: String,
}

/// Full routine signature read from a routine model YAML (create path).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutineModelUpdate {
    pub name: String,
    pub description: String,
    pub language: String,
    pub arguments: Vec<RoutineArgument>,
    pub return_type: Option<String>,
}

static SAVED_QUERY_HEADER_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^--\s*Saved Query:\s*(.+)$").unwrap());
static ROUTINE_NAME_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^--\s*Routine:\s*(.+)$").unwrap());
static ROUTINE_LANG_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^--\s*Language:\s*(.+)$").unwrap());

fn read_text(path: &Path) -> Result<String> {
    std::fs::read_to_string(path).map_err(|e| Error::io(path, e))
}

fn file_stem(path: &Path) -> String {
    stem(&path.to_string_lossy())
}

fn strip_eol(line: &str) -> &str {
    line.strip_suffix('\n').unwrap_or(line)
}

/// Parse a view ``.sql`` file (whole content is the SQL body).
pub fn read_view_sql(path: &Path) -> Result<ViewUpdate> {
    Ok(ViewUpdate {
        name: file_stem(path),
        sql: read_text(path)?,
    })
}

/// Parse a saved-query ``.sql`` file, stripping its ``-- Saved Query`` header.
pub fn read_saved_query_sql(path: &Path) -> Result<SavedQueryUpdate> {
    let text = read_text(path)?;
    Ok(parse_saved_query_text(&text, file_stem(path)))
}

fn parse_saved_query_text(text: &str, fallback_name: String) -> SavedQueryUpdate {
    let lines = split_lines_keepends(text);
    let mut name = fallback_name;
    let mut body_start = 0;
    for (idx, line) in lines.iter().enumerate() {
        if let Some(m) = SAVED_QUERY_HEADER_RE.captures(strip_eol(line)) {
            name = m[1].trim().to_owned();
            body_start = idx + 1;
            if body_start < lines.len() && lines[body_start].trim().is_empty() {
                body_start += 1;
            }
            break;
        }
    }
    SavedQueryUpdate {
        name,
        sql: lines[body_start..].concat(),
    }
}

/// Parse a routine ``.sql`` file, stripping its ``-- Routine`` /
/// ``-- Language`` headers.
pub fn read_routine_sql(path: &Path) -> Result<RoutineUpdate> {
    let text = read_text(path)?;
    Ok(parse_routine_text(&text, file_stem(path)))
}

fn parse_routine_text(text: &str, fallback_name: String) -> RoutineUpdate {
    let lines = split_lines_keepends(text);
    let mut name = fallback_name;
    let mut language = "SQL".to_owned();
    let mut body_start = 0;

    for (idx, line) in lines.iter().enumerate() {
        let stripped = strip_eol(line);
        if let Some(m) = ROUTINE_NAME_RE.captures(stripped) {
            name = m[1].trim().to_owned();
            body_start = idx + 1;
            continue;
        }
        if let Some(m) = ROUTINE_LANG_RE.captures(stripped) {
            language = m[1].trim().to_owned();
            body_start = idx + 1;
            continue;
        }
        if !stripped.trim().is_empty() {
            break;
        }
    }
    while body_start < lines.len() && lines[body_start].trim().is_empty() {
        body_start += 1;
    }
    RoutineUpdate {
        name,
        body: lines[body_start..].concat(),
        language,
    }
}

fn yaml_field<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    line.strip_prefix(key)?.strip_prefix(':').map(str::trim)
}

/// Parse a model ``.yaml`` (table, external or view model).
pub fn read_model_yaml(path: &Path) -> Result<ModelUpdate> {
    let text = read_text(path)?;
    Ok(parse_model_text(&text, file_stem(path)))
}

/// Parse a view model ``.yaml``; same layout as [`read_model_yaml`].
pub fn read_view_model_yaml(path: &Path) -> Result<ModelUpdate> {
    read_model_yaml(path)
}

/// Shared parser for model YAML text.
pub fn parse_model_text(text: &str, fallback_name: String) -> ModelUpdate {
    let mut out = ModelUpdate {
        name: fallback_name,
        ..Default::default()
    };
    let mut in_schema = false;
    for line in split_lines(text) {
        let stripped = line.trim();
        if !in_schema {
            if let Some(v) = yaml_field(stripped, "name") {
                out.name = v.to_owned();
            } else if let Some(v) = yaml_field(stripped, "description") {
                out.description = decode_json_str(v);
            } else if stripped == "schema:" {
                in_schema = true;
            }
            continue;
        }
        if ends_yaml_block(line) {
            break;
        }
        if stripped.starts_with("- name:") {
            let mut field = None;
            let mut desc = String::new();
            for part in yaml_entry_parts(stripped) {
                if let Some(v) = part.strip_prefix("name:") {
                    field = Some(v.trim().to_owned());
                } else if let Some(v) = part.strip_prefix("description:") {
                    desc = decode_json_str(v.trim());
                }
            }
            if let Some(f) = field.filter(|f| !f.is_empty())
                && !desc.is_empty()
            {
                out.field_descriptions.insert(f, desc);
            }
        }
    }
    out
}

/// Parse a routine model YAML (signature used on the create path).
pub fn read_routine_model_yaml(path: &Path) -> Result<RoutineModelUpdate> {
    let text = read_text(path)?;
    Ok(parse_routine_model_text(&text, file_stem(path)))
}

fn parse_routine_model_text(text: &str, fallback_name: String) -> RoutineModelUpdate {
    let mut out = RoutineModelUpdate {
        name: fallback_name,
        description: String::new(),
        language: "SQL".into(),
        arguments: Vec::new(),
        return_type: None,
    };
    let mut in_arguments = false;
    for line in split_lines(text) {
        let stripped = line.trim();
        if !in_arguments {
            if let Some(v) = yaml_field(stripped, "name") {
                out.name = v.to_owned();
            } else if let Some(v) = yaml_field(stripped, "description") {
                out.description = decode_json_str(v);
            } else if let Some(v) = yaml_field(stripped, "language") {
                out.language = v.to_owned();
            } else if let Some(v) = yaml_field(stripped, "return_type") {
                out.return_type = Some(v.to_owned());
            } else if stripped == "arguments:" {
                in_arguments = true;
            }
            continue;
        }
        if ends_yaml_block(line) {
            break;
        }
        if stripped.starts_with("- name:") {
            let mut arg = RoutineArgument::default();
            for part in yaml_entry_parts(stripped) {
                if let Some(v) = part.strip_prefix("name:") {
                    arg.name = v.trim().to_owned();
                } else if let Some(v) = part.strip_prefix("type:") {
                    arg.arg_type = v.trim().to_owned();
                } else if let Some(v) = part.strip_prefix("mode:") {
                    arg.mode = v.trim().to_owned();
                }
            }
            out.arguments.push(arg);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::{RoutineInfo, SavedQueryInfo, SchemaField, TableInfo, ViewInfo};
    use crate::writers;
    use chrono::{TimeZone, Utc};

    fn ts() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap()
    }

    #[test]
    fn view_sql_roundtrip_and_plain() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("my_view.sql");
        let v = ViewInfo {
            name: "my_view".into(),
            sql: "SELECT id FROM users".into(),
            modified: ts(),
            schema: vec![],
            description: String::new(),
            created: None,
            region: None,
        };
        writers::write_view_sql(&p, &v).unwrap();
        let u = read_view_sql(&p).unwrap();
        assert_eq!(u.name, "my_view");
        assert_eq!(u.sql, "SELECT id FROM users");
    }

    #[test]
    fn saved_query_roundtrip_and_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("q1.sql");
        let s = SavedQueryInfo {
            name: "Monthly Report".into(),
            sql: "SELECT 2\n".into(),
            modified: ts(),
        };
        writers::write_saved_query_sql(&p, &s).unwrap();
        let u = read_saved_query_sql(&p).unwrap();
        assert_eq!(u.name, "Monthly Report");
        assert_eq!(u.sql, "SELECT 2\n");

        let p = dir.path().join("fallback.sql");
        std::fs::write(&p, "SELECT 3").unwrap();
        let u = read_saved_query_sql(&p).unwrap();
        assert_eq!(u.name, "fallback");
        assert_eq!(u.sql, "SELECT 3");
    }

    #[test]
    fn model_yaml_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("events.yaml");
        let t = TableInfo {
            name: "events".into(),
            schema: vec![
                SchemaField::new("id", "INTEGER", "REQUIRED", "Event ID"),
                SchemaField::new("ts", "TIMESTAMP", "NULLABLE", ""),
                SchemaField::new("note", "STRING", "NULLABLE", "Has  two spaces? no"),
            ],
            description: "Event log".into(),
            row_count: 10,
            modified: ts(),
            ..Default::default()
        };
        writers::write_model_yaml(&p, &t).unwrap();
        let u = read_model_yaml(&p).unwrap();
        assert_eq!(u.name, "events");
        assert_eq!(u.description, "Event log");
        assert_eq!(u.field_descriptions.get("id").unwrap(), "Event ID");
        assert!(!u.field_descriptions.contains_key("ts"));
    }

    #[test]
    fn model_yaml_without_descriptions() {
        let u = parse_model_text(
            "name: t\ndescription: \"\"\nschema:\n  - name: a  type: STRING  mode: NULLABLE  description: \"\"\n",
            "x".into(),
        );
        assert_eq!(u.description, "");
        assert!(u.field_descriptions.is_empty());
    }

    #[test]
    fn routine_sql_sql_and_js() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("fn.sql");
        let r = RoutineInfo {
            name: "fn".into(),
            sql: "RETURN 1;".into(),
            language: "SQL".into(),
            modified: ts(),
            description: String::new(),
            created: None,
            arguments: vec![],
            return_type: None,
        };
        writers::write_routine_sql(&p, &r).unwrap();
        let u = read_routine_sql(&p).unwrap();
        assert_eq!(
            (u.name.as_str(), u.body.as_str(), u.language.as_str()),
            ("fn", "RETURN 1;", "SQL")
        );

        let r = RoutineInfo {
            name: "js_fn".into(),
            sql: "return x * 2;".into(),
            language: "JAVASCRIPT".into(),
            ..r
        };
        writers::write_routine_sql(&p, &r).unwrap();
        let u = read_routine_sql(&p).unwrap();
        assert_eq!(u.name, "js_fn");
        assert_eq!(u.language, "JAVASCRIPT");
        assert_eq!(u.body, "return x * 2;");

        let p = dir.path().join("bare.sql");
        std::fs::write(&p, "SELECT 1").unwrap();
        let u = read_routine_sql(&p).unwrap();
        assert_eq!((u.name.as_str(), u.language.as_str()), ("bare", "SQL"));
    }

    #[test]
    fn routine_model_yaml_parse() {
        let m = parse_routine_model_text(
            "name: add\ndescription: \"Adds\"\nlanguage: SQL\nreturn_type: INT64\narguments:\n  - name: x  type: INT64  mode: IN\n  - name: y  type: INT64  mode: IN\n",
            "f".into(),
        );
        assert_eq!(m.name, "add");
        assert_eq!(m.description, "Adds");
        assert_eq!(m.return_type.as_deref(), Some("INT64"));
        assert_eq!(m.arguments.len(), 2);
        assert_eq!(m.arguments[1].name, "y");
        assert_eq!(m.arguments[1].arg_type, "INT64");
    }
}
