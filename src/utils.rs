//! Shared helpers used across bq-sync modules.

use std::io::{BufRead, Write};
use std::path::Path;

use chrono::{DateTime, Timelike, Utc};
use unicode_normalization::UnicodeNormalization;

use crate::error::{Error, Result};

/// Resource directory names recognised in the local output tree.
pub const RESOURCE_DIRS: [&str; 4] = ["models", "views", "routines", "saved_queries"];

/// Return the file stem of *name* (last extension stripped), like
/// Python's ``Path(name).stem``.
pub fn stem(name: &str) -> String {
    Path::new(name)
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// Longest file name stem, in bytes, that fits common filesystem limits
/// together with an extension.
const MAX_STEM_BYTES: usize = 200;

/// File name stem for the free-text resource *name*: the characters a path
/// cannot hold are replaced so the file stays inside the directory it is
/// joined to, and the stem is cut to [`MAX_STEM_BYTES`].
pub fn file_stem_of(name: &str) -> String {
    let mut stem = name.replace(['/', '\\', '\0'], "_");
    while stem.len() > MAX_STEM_BYTES {
        stem.pop();
    }
    stem
}

/// Parse a CLI resource path into ``(project, dataset, name)``.
///
/// Accepted forms:
///
/// - BQ tree: ``<project>/<dataset>/<name>[.yaml|.sql]``
/// - Local tree: ``<project>/<dataset>/<resource_dir>/<name>[.yaml|.sql]``
///   (the ``<resource_dir>`` segment is dropped).
///
/// An invalid path is logged and reported as [`Error::Exit`].
pub fn parse_resource_path(resource_path: &str) -> Result<(String, String, String)> {
    let parts: Vec<&str> = resource_path.split('/').collect();
    match parts.as_slice() {
        [project, dataset, name] => Ok(((*project).into(), (*dataset).into(), stem(name))),
        [project, dataset, resource_dir, name] => {
            if !RESOURCE_DIRS.contains(resource_dir) {
                log::warn!(
                    "Unknown resource directory '{resource_dir}' in path '{resource_path}'; \
                     treating as a BQ path segment."
                );
            }
            Ok(((*project).into(), (*dataset).into(), stem(name)))
        }
        _ => {
            log::error!(
                "Invalid resource path '{resource_path}': expected \
                 <project>/<dataset>/<name> or \
                 <project>/<dataset>/<resource_dir>/<name>."
            );
            Err(Error::Exit(1))
        }
    }
}

/// Convert a byte count to a human-readable string using binary prefixes.
pub fn humanize_bytes(n: Option<i64>) -> String {
    let Some(n) = n else {
        return String::new();
    };
    if n < 0 {
        return format!("{n} B");
    }
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut value = n as f64;
    for unit in &UNITS[..UNITS.len() - 1] {
        if value.abs() < 1024.0 {
            return if *unit == "B" {
                format!("{n} B")
            } else {
                format!("{value:.1} {unit}")
            };
        }
        value /= 1024.0;
    }
    format!("{value:.1} {}", UNITS[UNITS.len() - 1])
}

/// Normalise Unicode text to NFC (composed) form.
///
/// BigQuery may return accented strings in NFD form; storing NFC keeps
/// file bytes stable across pulls.
pub fn normalize_text(s: &str) -> String {
    s.nfc().collect()
}

/// Format a UTC timestamp like Python's ``datetime.isoformat()``
/// (microseconds only when non-zero, ``+00:00`` offset).
pub fn isoformat(dt: &DateTime<Utc>) -> String {
    let micros = dt.nanosecond() / 1_000 % 1_000_000;
    let base = dt.format("%Y-%m-%dT%H:%M:%S");
    if micros == 0 {
        format!("{base}+00:00")
    } else {
        format!("{base}.{micros:06}+00:00")
    }
}

/// JSON-encode a string without escaping non-ASCII characters
/// (``json.dumps(s, ensure_ascii=False)``).
pub fn json_str(s: &str) -> String {
    serde_json::to_string(s).expect("string serialization cannot fail")
}

/// Decode a JSON-encoded string value, falling back to the raw text.
pub fn decode_json_str(raw: &str) -> String {
    if raw.is_empty() {
        return String::new();
    }
    match serde_json::from_str::<serde_json::Value>(raw) {
        Ok(serde_json::Value::String(s)) => s,
        Ok(other) => other.to_string(),
        Err(_) => raw.to_owned(),
    }
}

/// Render a string like Python's ``repr()`` (used in diagnostics).
pub fn py_repr(s: &str) -> String {
    let quote = if s.contains('\'') && !s.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut out = String::with_capacity(s.len() + 2);
    out.push(quote);
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            c if (c as u32) < 0x20 || c as u32 == 0x7f => {
                out.push_str(&format!("\\x{:02x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push(quote);
    out
}

/// Prompt for ``[y/N]`` confirmation on stdin (default: deny).
pub fn confirm(prompt: &str) -> bool {
    confirm_from(prompt, &mut std::io::stdin().lock())
}

/// [`confirm`] reading the answer from *input*.
pub fn confirm_from(prompt: &str, input: &mut dyn BufRead) -> bool {
    print!("  {prompt} [y/N] ");
    let _ = std::io::stdout().flush();
    let mut answer = String::new();
    match input.read_line(&mut answer) {
        Ok(0) | Err(_) => {
            println!();
            false
        }
        Ok(_) => matches!(answer.trim().to_lowercase().as_str(), "y" | "yes"),
    }
}

/// Split a line into fields separated by two spaces, the format used by
/// the schema / argument entries of model YAML files.
pub(crate) fn yaml_entry_parts(stripped: &str) -> impl Iterator<Item = &str> {
    stripped
        .split("  ")
        .map(|part| part.trim().trim_start_matches(['-', ' ']))
}

/// Whether *line* closes an indented YAML block (a top-level key).
pub(crate) fn ends_yaml_block(line: &str) -> bool {
    let stripped = line.trim();
    !line.starts_with(' ') && !stripped.is_empty() && !stripped.starts_with('-')
}

/// Python ``str.splitlines()`` without line terminators.
pub(crate) fn split_lines(text: &str) -> Vec<&str> {
    text.lines().collect()
}

/// Python ``str.splitlines(keepends=True)``.
pub(crate) fn split_lines_keepends(text: &str) -> Vec<&str> {
    text.split_inclusive('\n').collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn resource_path_forms() {
        let p = |s| parse_resource_path(s).unwrap();
        let t = |a: &str, b: &str, c: &str| (a.to_owned(), b.to_owned(), c.to_owned());
        assert_eq!(p("proj/ds/tbl"), t("proj", "ds", "tbl"));
        assert_eq!(p("proj/ds/tbl.yaml"), t("proj", "ds", "tbl"));
        assert_eq!(p("proj/ds/v.sql"), t("proj", "ds", "v"));
        assert_eq!(p("proj/ds/models/tbl.yaml"), t("proj", "ds", "tbl"));
        assert_eq!(p("proj/ds/views/v.sql"), t("proj", "ds", "v"));
        assert!(matches!(parse_resource_path("a/b"), Err(Error::Exit(1))));
    }

    #[test]
    fn humanize() {
        assert_eq!(humanize_bytes(None), "");
        assert_eq!(humanize_bytes(Some(0)), "0 B");
        assert_eq!(humanize_bytes(Some(512)), "512 B");
        assert_eq!(humanize_bytes(Some(1024)), "1.0 KiB");
        assert_eq!(humanize_bytes(Some(1024 * 1024)), "1.0 MiB");
        assert_eq!(humanize_bytes(Some(2 * 1024_i64.pow(3))), "2.0 GiB");
        assert_eq!(humanize_bytes(Some(5 * 1024_i64.pow(3))), "5.0 GiB");
        assert_eq!(humanize_bytes(Some(-1)), "-1 B");
    }

    #[test]
    fn isoformat_matches_python() {
        let dt = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        assert_eq!(isoformat(&dt), "2025-01-01T00:00:00+00:00");
        let dt = dt + chrono::Duration::milliseconds(789);
        assert_eq!(isoformat(&dt), "2025-01-01T00:00:00.789000+00:00");
    }

    #[test]
    fn json_helpers() {
        assert_eq!(json_str("declaración"), "\"declaración\"");
        assert_eq!(decode_json_str("\"a\\\"b\""), "a\"b");
        assert_eq!(decode_json_str("plain"), "plain");
        assert_eq!(decode_json_str(""), "");
    }

    #[test]
    fn repr() {
        assert_eq!(py_repr("x {"), "'x {'");
        assert_eq!(py_repr("it's"), "\"it's\"");
        assert_eq!(py_repr("a\nb"), "'a\\nb'");
    }

    #[test]
    fn file_stem_of_drops_separators() {
        assert_eq!(file_stem_of("daily load"), "daily load");
        assert_eq!(file_stem_of("a/b\\c"), "a_b_c");
        assert_eq!(file_stem_of("../x"), ".._x");
        assert_eq!(file_stem_of("a\nb\0c"), "a\nb_c");
        assert_eq!(file_stem_of(&"é".repeat(150)).len(), 200);
    }

    #[test]
    fn confirm_answers() {
        let mut yes = "y\n".as_bytes();
        assert!(confirm_from("ok?", &mut yes));
        let mut no = "n\n".as_bytes();
        assert!(!confirm_from("ok?", &mut no));
        let mut empty = "\n".as_bytes();
        assert!(!confirm_from("ok?", &mut empty));
        let mut eof = "".as_bytes();
        assert!(!confirm_from("ok?", &mut eof));
    }
}
