//! Write BigQuery resources to the local filesystem (pull mode).
//!
//! Model files use a minimal, line-oriented YAML dialect (one schema field
//! per line, descriptions JSON-encoded) that the readers parse back without
//! a YAML library.

use std::collections::HashMap;
use std::path::Path;

use crate::error::{Error, Result};
use crate::resources::{
    ExternalTableInfo, RoutineInfo, SavedQueryInfo, ScheduledQueryInfo, SchemaField, TableInfo,
    ViewInfo,
};
use crate::utils::{
    decode_json_str, ends_yaml_block, humanize_bytes, isoformat, json_str, normalize_text,
    split_lines, yaml_entry_parts,
};

fn write_file(path: &Path, content: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| Error::io(parent, e))?;
    }
    std::fs::write(path, content).map_err(|e| Error::io(path, e))
}

/// Extract the top-level and per-field descriptions from an existing model
/// YAML file.  Returns ``(None, {})`` when the file does not exist.
pub fn load_existing_descriptions(path: &Path) -> (Option<String>, HashMap<String, String>) {
    let Ok(raw) = std::fs::read_to_string(path) else {
        return (None, HashMap::new());
    };
    // Normalise on read-back so merge comparisons are NFC vs NFC.
    let text = normalize_text(&raw);
    let lines = split_lines(&text);

    let model_desc = lines.iter().find_map(|line| {
        line.trim()
            .strip_prefix("description:")
            .map(|raw| decode_json_str(raw.trim()))
    });
    let model_desc = model_desc.filter(|d| !d.is_empty());

    let mut field_descs = HashMap::new();
    let mut in_schema = false;
    for line in &lines {
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
            let mut current = None;
            let mut desc = String::new();
            for part in yaml_entry_parts(stripped) {
                if let Some(v) = part.strip_prefix("name:") {
                    current = Some(v.trim().to_owned());
                } else if let Some(v) = part.strip_prefix("description:") {
                    desc = decode_json_str(v.trim());
                }
            }
            if let Some(name) = current.filter(|n| !n.is_empty())
                && !desc.is_empty()
            {
                field_descs.insert(name, desc);
            }
        }
    }
    (model_desc, field_descs)
}

/// Prefer a non-empty local description over the BigQuery one (NFC).
fn merge_description(bq_desc: &str, local_desc: Option<&str>) -> String {
    normalize_text(local_desc.filter(|d| !d.is_empty()).unwrap_or(bq_desc))
}

fn merge_field_descriptions(
    schema: &[SchemaField],
    local: &HashMap<String, String>,
) -> Vec<SchemaField> {
    schema
        .iter()
        .map(|f| match local.get(&f.name) {
            Some(desc) => SchemaField {
                description: desc.clone(),
                ..f.clone()
            },
            None => f.clone(),
        })
        .collect()
}

fn schema_lines(schema: &[SchemaField], lines: &mut Vec<String>) {
    lines.push("schema:".into());
    for f in schema {
        lines.push(format!(
            "  - name: {}  type: {}  mode: {}  description: {}",
            f.name,
            f.field_type,
            f.mode,
            json_str(&normalize_text(&f.description)),
        ));
    }
}

fn finish(lines: Vec<String>) -> String {
    lines.join("\n") + "\n"
}

/// Write a view SQL definition.
pub fn write_view_sql(path: &Path, view: &ViewInfo) -> Result<()> {
    write_file(path, &normalize_text(&view.sql))
}

/// Write a routine body with its ``-- Routine`` / ``-- Language`` header.
pub fn write_routine_sql(path: &Path, routine: &RoutineInfo) -> Result<()> {
    let header = format!(
        "-- Routine: {}\n-- Language: {}\n\n",
        routine.name, routine.language
    );
    write_file(path, &(header + &normalize_text(&routine.sql)))
}

/// Write a scheduled query with its name / schedule header.
pub fn write_scheduled_query_sql(path: &Path, sq: &ScheduledQueryInfo) -> Result<()> {
    let header = format!(
        "-- Scheduled Query: {}\n-- Schedule: {}\n\n",
        sq.name, sq.schedule
    );
    write_file(path, &(header + &normalize_text(&sq.sql)))
}

/// Write a saved query with its name header.
pub fn write_saved_query_sql(path: &Path, saved: &SavedQueryInfo) -> Result<()> {
    let header = format!("-- Saved Query: {}\n\n", saved.name);
    write_file(path, &(header + &normalize_text(&saved.sql)))
}

fn push_table_extras(
    lines: &mut Vec<String>,
    partitioning: &Option<String>,
    clustering: &Option<Vec<String>>,
    primary_keys: &Option<Vec<String>>,
    total_logical_bytes: Option<i64>,
) {
    if let Some(p) = partitioning.as_deref().filter(|p| !p.is_empty()) {
        lines.push(format!("partitioning: {p}"));
    }
    if let Some(c) = clustering.as_ref().filter(|c| !c.is_empty()) {
        lines.push(format!("clustering: [{}]", c.join(", ")));
    }
    if let Some(pk) = primary_keys.as_ref().filter(|pk| !pk.is_empty()) {
        lines.push(format!("primary_keys: [{}]", pk.join(", ")));
    }
    if total_logical_bytes.is_some() {
        lines.push(format!(
            "total_logical_bytes: {}",
            humanize_bytes(total_logical_bytes)
        ));
    }
}

/// Write table metadata as a model YAML, preserving local descriptions.
pub fn write_model_yaml(path: &Path, table: &TableInfo) -> Result<()> {
    let (local_desc, local_fields) = load_existing_descriptions(path);
    let desc = merge_description(&table.description, local_desc.as_deref());
    let schema = merge_field_descriptions(&table.schema, &local_fields);

    let mut lines = vec![
        format!("name: {}", table.name),
        format!("description: {}", json_str(&desc)),
        format!("row_count: {}", table.row_count),
    ];
    if let Some(created) = &table.created {
        lines.push(format!("created: {}", isoformat(created)));
    }
    lines.push(format!("modified: {}", isoformat(&table.modified)));
    if let Some(region) = table.region.as_deref().filter(|r| !r.is_empty()) {
        lines.push(format!("region: {region}"));
    }
    push_table_extras(
        &mut lines,
        &table.partitioning,
        &table.clustering,
        &table.primary_keys,
        table.total_logical_bytes,
    );
    schema_lines(&schema, &mut lines);
    write_file(path, &finish(lines))
}

/// Write view metadata as a model YAML (``type: VIEW``).
pub fn write_view_model_yaml(path: &Path, view: &ViewInfo) -> Result<()> {
    let (local_desc, local_fields) = load_existing_descriptions(path);
    let desc = merge_description(&view.description, local_desc.as_deref());
    let schema = merge_field_descriptions(&view.schema, &local_fields);

    let mut lines = vec![
        format!("name: {}", view.name),
        format!("description: {}", json_str(&desc)),
        "type: VIEW".into(),
    ];
    if let Some(created) = &view.created {
        lines.push(format!("created: {}", isoformat(created)));
    }
    lines.push(format!("modified: {}", isoformat(&view.modified)));
    if let Some(region) = view.region.as_deref().filter(|r| !r.is_empty()) {
        lines.push(format!("region: {region}"));
    }
    schema_lines(&schema, &mut lines);
    write_file(path, &finish(lines))
}

/// Write routine metadata (language, signature) as a model YAML.
pub fn write_routine_model_yaml(path: &Path, routine: &RoutineInfo) -> Result<()> {
    let (local_desc, _) = load_existing_descriptions(path);
    let desc = merge_description(&routine.description, local_desc.as_deref());

    let mut lines = vec![
        format!("name: {}", routine.name),
        format!("description: {}", json_str(&desc)),
        format!("language: {}", routine.language),
    ];
    if let Some(created) = &routine.created {
        lines.push(format!("created: {}", isoformat(created)));
    }
    lines.push(format!("modified: {}", isoformat(&routine.modified)));
    if let Some(rt) = routine.return_type.as_deref().filter(|r| !r.is_empty()) {
        lines.push(format!("return_type: {rt}"));
    }
    if !routine.arguments.is_empty() {
        lines.push("arguments:".into());
        for arg in &routine.arguments {
            lines.push(format!(
                "  - name: {}  type: {}  mode: {}",
                arg.name, arg.arg_type, arg.mode
            ));
        }
    }
    write_file(path, &finish(lines))
}

/// Write an external table definition as YAML.
pub fn write_external_definition(path: &Path, ext: &ExternalTableInfo) -> Result<()> {
    let (local_desc, local_fields) = load_existing_descriptions(path);
    let desc = merge_description(&ext.description, local_desc.as_deref());
    let schema = merge_field_descriptions(&ext.schema, &local_fields);

    let mut lines = vec![
        format!("name: {}", ext.name),
        format!("description: {}", json_str(&desc)),
        format!("source_format: {}", ext.source_format),
        "source_uris:".into(),
    ];
    lines.extend(ext.source_uris.iter().map(|u| format!("  - {u}")));
    if let Some(created) = &ext.created {
        lines.push(format!("created: {}", isoformat(created)));
    }
    lines.push(format!("modified: {}", isoformat(&ext.modified)));
    if let Some(region) = ext.region.as_deref().filter(|r| !r.is_empty()) {
        lines.push(format!("region: {region}"));
    }
    lines.push(format!("row_count: {}", ext.row_count));
    push_table_extras(
        &mut lines,
        &ext.partitioning,
        &ext.clustering,
        &ext.primary_keys,
        ext.total_logical_bytes,
    );
    schema_lines(&schema, &mut lines);
    write_file(path, &finish(lines))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::{EPOCH, RoutineArgument};
    use chrono::{DateTime, TimeZone, Utc};

    fn ts() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap()
    }
    fn ts_created() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2024, 6, 15, 12, 0, 0).unwrap()
    }
    fn read(p: &Path) -> String {
        std::fs::read_to_string(p).unwrap()
    }
    fn view(name: &str, sql: &str) -> ViewInfo {
        ViewInfo {
            name: name.into(),
            sql: sql.into(),
            modified: ts(),
            schema: vec![],
            description: String::new(),
            created: None,
            region: None,
        }
    }
    fn routine(name: &str, sql: &str) -> RoutineInfo {
        RoutineInfo {
            name: name.into(),
            sql: sql.into(),
            language: "SQL".into(),
            modified: ts(),
            description: String::new(),
            created: None,
            arguments: vec![],
            return_type: None,
        }
    }

    #[test]
    fn view_sql_verbatim() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("views/my_view.sql");
        write_view_sql(&p, &view("my_view", "SELECT 1")).unwrap();
        assert_eq!(read(&p), "SELECT 1");
    }

    #[test]
    fn view_model_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("models/active_users.yaml");
        let v = ViewInfo {
            schema: vec![SchemaField::new("id", "INTEGER", "REQUIRED", "User ID")],
            description: "Active users view".into(),
            created: Some(ts_created()),
            region: Some("us-east1".into()),
            ..view("active_users", "SELECT id FROM users")
        };
        write_view_model_yaml(&p, &v).unwrap();
        assert_eq!(
            read(&p),
            "name: active_users\n\
             description: \"Active users view\"\n\
             type: VIEW\n\
             created: 2024-06-15T12:00:00+00:00\n\
             modified: 2025-01-01T00:00:00+00:00\n\
             region: us-east1\n\
             schema:\n  \
             - name: id  type: INTEGER  mode: REQUIRED  description: \"User ID\"\n"
        );
    }

    #[test]
    fn routine_sql_header() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("routines/fn.sql");
        write_routine_sql(&p, &routine("fn", "RETURN 1;")).unwrap();
        assert_eq!(read(&p), "-- Routine: fn\n-- Language: SQL\n\nRETURN 1;");
    }

    #[test]
    fn scheduled_and_saved_headers() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("sq/daily_load.sql");
        let sq = ScheduledQueryInfo {
            name: "daily_load".into(),
            sql: "INSERT ...".into(),
            schedule: "every 24 hours".into(),
            modified: ts(),
        };
        write_scheduled_query_sql(&p, &sq).unwrap();
        assert_eq!(
            read(&p),
            "-- Scheduled Query: daily_load\n-- Schedule: every 24 hours\n\nINSERT ..."
        );

        let p = dir.path().join("saved/q1.sql");
        let saved = SavedQueryInfo {
            name: "q1".into(),
            sql: "SELECT 2".into(),
            modified: EPOCH,
        };
        write_saved_query_sql(&p, &saved).unwrap();
        assert_eq!(read(&p), "-- Saved Query: q1\n\nSELECT 2");
    }

    #[test]
    fn model_yaml_full() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("models/events.yaml");
        let t = TableInfo {
            name: "events".into(),
            schema: vec![
                SchemaField::new("id", "INTEGER", "REQUIRED", "Event ID"),
                SchemaField::new("ts", "TIMESTAMP", "NULLABLE", ""),
            ],
            description: "Event log".into(),
            row_count: 1000,
            modified: ts(),
            partitioning: Some("ts".into()),
            clustering: Some(vec!["id".into()]),
            created: Some(ts_created()),
            region: Some("US".into()),
            primary_keys: Some(vec!["id".into()]),
            total_logical_bytes: Some(5 * 1024_i64.pow(3)),
        };
        write_model_yaml(&p, &t).unwrap();
        let content = read(&p);
        assert_eq!(
            content,
            "name: events\n\
             description: \"Event log\"\n\
             row_count: 1000\n\
             created: 2024-06-15T12:00:00+00:00\n\
             modified: 2025-01-01T00:00:00+00:00\n\
             region: US\n\
             partitioning: ts\n\
             clustering: [id]\n\
             primary_keys: [id]\n\
             total_logical_bytes: 5.0 GiB\n\
             schema:\n\
             \x20 - name: id  type: INTEGER  mode: REQUIRED  description: \"Event ID\"\n\
             \x20 - name: ts  type: TIMESTAMP  mode: NULLABLE  description: \"\"\n"
        );
        assert_eq!(content.matches("description:").count(), 3);
    }

    #[test]
    fn non_ascii_written_as_utf8() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("models/declaraciones.yaml");
        let t = TableInfo {
            name: "declaraciones".into(),
            schema: vec![SchemaField::new(
                "tipo",
                "STRING",
                "NULLABLE",
                "Tipo de declaración",
            )],
            description: "Información de declaración".into(),
            modified: ts(),
            ..Default::default()
        };
        write_model_yaml(&p, &t).unwrap();
        let c = read(&p);
        assert!(c.contains("declaración"));
        assert!(!c.contains("\\u00f3"));
    }

    #[test]
    fn external_definition() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("externals/ext_table.yaml");
        let e = ExternalTableInfo {
            name: "ext_table".into(),
            source_uris: vec!["gs://bucket/file.csv".into()],
            schema: vec![SchemaField::new("col", "STRING", "NULLABLE", "A column")],
            source_format: "CSV".into(),
            modified: ts(),
            description: "External feed".into(),
            created: Some(ts_created()),
            region: Some("EU".into()),
            total_logical_bytes: Some(2048),
            row_count: 50,
            ..Default::default()
        };
        write_external_definition(&p, &e).unwrap();
        assert_eq!(
            read(&p),
            "name: ext_table\n\
             description: \"External feed\"\n\
             source_format: CSV\n\
             source_uris:\n\
             \x20 - gs://bucket/file.csv\n\
             created: 2024-06-15T12:00:00+00:00\n\
             modified: 2025-01-01T00:00:00+00:00\n\
             region: EU\n\
             row_count: 50\n\
             total_logical_bytes: 2.0 KiB\n\
             schema:\n\
             \x20 - name: col  type: STRING  mode: NULLABLE  description: \"A column\"\n"
        );
    }

    #[test]
    fn routine_model_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("models/add_numbers.yaml");
        let r = RoutineInfo {
            description: "Adds two numbers".into(),
            created: Some(ts_created()),
            arguments: vec![
                RoutineArgument {
                    name: "x".into(),
                    arg_type: "INT64".into(),
                    mode: "IN".into(),
                },
                RoutineArgument {
                    name: "y".into(),
                    arg_type: "INT64".into(),
                    mode: "IN".into(),
                },
            ],
            return_type: Some("INT64".into()),
            ..routine("add_numbers", "RETURN x + y;")
        };
        write_routine_model_yaml(&p, &r).unwrap();
        assert_eq!(
            read(&p),
            "name: add_numbers\n\
             description: \"Adds two numbers\"\n\
             language: SQL\n\
             created: 2024-06-15T12:00:00+00:00\n\
             modified: 2025-01-01T00:00:00+00:00\n\
             return_type: INT64\n\
             arguments:\n\
             \x20 - name: x  type: INT64  mode: IN\n\
             \x20 - name: y  type: INT64  mode: IN\n"
        );
    }

    fn seed(dir: &Path, name: &str, content: &str) -> std::path::PathBuf {
        let p = dir.join("models").join(name);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, content).unwrap();
        p
    }

    #[test]
    fn preserves_local_descriptions() {
        let dir = tempfile::tempdir().unwrap();
        let base = TableInfo {
            name: "events".into(),
            description: "BQ doc".into(),
            modified: ts(),
            ..Default::default()
        };

        let p = seed(
            dir.path(),
            "events.yaml",
            "name: events\ndescription: \"Local doc\"\nschema:\n",
        );
        write_model_yaml(&p, &base).unwrap();
        let c = read(&p);
        assert!(c.contains("\"Local doc\"") && !c.contains("\"BQ doc\""));

        let p = seed(
            dir.path(),
            "events.yaml",
            "name: events\ndescription: \"\"\nschema:\n",
        );
        write_model_yaml(&p, &base).unwrap();
        assert!(read(&p).contains("\"BQ doc\""));

        let p = seed(
            dir.path(),
            "events.yaml",
            "name: events\ndescription: \"\"\nschema:\n  - name: id  type: INTEGER  mode: REQUIRED  description: \"Local field doc\"\n",
        );
        let t = TableInfo {
            schema: vec![SchemaField::new(
                "id",
                "INTEGER",
                "REQUIRED",
                "BQ field doc",
            )],
            description: String::new(),
            ..base.clone()
        };
        write_model_yaml(&p, &t).unwrap();
        let c = read(&p);
        assert!(c.contains("\"Local field doc\"") && !c.contains("\"BQ field doc\""));

        let p = seed(
            dir.path(),
            "v.yaml",
            "name: v\ndescription: \"Local view\"\ntype: VIEW\nschema:\n",
        );
        let v = ViewInfo {
            description: "BQ view".into(),
            ..view("v", "SELECT 1")
        };
        write_view_model_yaml(&p, &v).unwrap();
        assert!(read(&p).contains("\"Local view\""));

        let p = seed(
            dir.path(),
            "fn.yaml",
            "name: fn\ndescription: \"Local routine\"\nlanguage: SQL\n",
        );
        let r = RoutineInfo {
            description: "BQ routine".into(),
            ..routine("fn", "RETURN 1;")
        };
        write_routine_model_yaml(&p, &r).unwrap();
        assert!(read(&p).contains("\"Local routine\""));

        let p = seed(
            dir.path(),
            "ext.yaml",
            "name: ext\ndescription: \"Local ext\"\nschema:\n",
        );
        let e = ExternalTableInfo {
            name: "ext".into(),
            source_uris: vec!["gs://b/f.csv".into()],
            source_format: "CSV".into(),
            modified: ts(),
            description: "BQ ext".into(),
            ..Default::default()
        };
        write_external_definition(&p, &e).unwrap();
        let c = read(&p);
        assert!(c.contains("\"Local ext\"") && !c.contains("\"BQ ext\""));
    }

    #[test]
    fn nfd_normalised_and_stable() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("v.sql");
        write_view_sql(&p, &view("v", "SELECT `nio\u{301}t` FROM users")).unwrap();
        assert!(read(&p).contains("SELECT `ni\u{f3}t` FROM users"));

        let p = dir.path().join("t.yaml");
        let t = TableInfo {
            name: "t".into(),
            schema: vec![SchemaField::new(
                "col",
                "STRING",
                "NULLABLE",
                "Declaracio\u{301}n fiscal",
            )],
            description: "Descripcio\u{303}n de la tabla".into(),
            modified: ts(),
            ..Default::default()
        };
        write_model_yaml(&p, &t).unwrap();
        let first = std::fs::read(&p).unwrap();
        assert!(read(&p).contains(&normalize_text("Descripcio\u{303}n")));
        write_model_yaml(&p, &t).unwrap();
        assert_eq!(first, std::fs::read(&p).unwrap());
    }
}
