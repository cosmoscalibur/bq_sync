//! Convert BigQuery REST row payloads into Arrow and write CSV / Parquet.
//!
//! Scalar columns map to typed Arrow arrays; ``REPEATED`` and ``RECORD``
//! columns are serialised as JSON text.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder, Int64Builder,
    StringBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use serde_json::{Map, Value};

use crate::error::{Error, Result};
use crate::resources::DataFormat;

/// NUMERIC precision / scale (BigQuery ``NUMERIC`` is DECIMAL(38, 9)).
const NUMERIC_PRECISION: u8 = 38;
const NUMERIC_SCALE: i8 = 9;

/// A field of a BigQuery table / query result schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BqField {
    pub name: String,
    pub field_type: String,
    pub mode: String,
    pub fields: Vec<BqField>,
}

/// Upper-cased column *mode*; BigQuery omits it for ``NULLABLE`` columns.
pub fn normalize_mode(mode: &str) -> String {
    if mode.is_empty() {
        "NULLABLE".to_owned()
    } else {
        mode.to_uppercase()
    }
}

impl BqField {
    /// Parse a REST ``schema`` object (``{"fields": [...]}``).
    pub fn from_schema(schema: Option<&Value>) -> Vec<BqField> {
        schema
            .and_then(|s| s.get("fields"))
            .and_then(Value::as_array)
            .map(|fields| fields.iter().map(BqField::from_json).collect())
            .unwrap_or_default()
    }

    fn from_json(v: &Value) -> BqField {
        let s = |k: &str| v.get(k).and_then(Value::as_str).unwrap_or_default();
        BqField {
            name: s("name").to_owned(),
            field_type: s("type").to_uppercase(),
            mode: normalize_mode(s("mode")),
            fields: BqField::from_schema(Some(v)),
        }
    }

    fn is_nested(&self) -> bool {
        self.mode == "REPEATED" || matches!(self.field_type.as_str(), "RECORD" | "STRUCT")
    }

    fn arrow_type(&self, fmt: DataFormat) -> DataType {
        if self.is_nested() {
            return DataType::Utf8;
        }
        match self.field_type.as_str() {
            "INTEGER" | "INT64" => DataType::Int64,
            "FLOAT" | "FLOAT64" => DataType::Float64,
            "BOOLEAN" | "BOOL" => DataType::Boolean,
            "NUMERIC" => DataType::Decimal128(NUMERIC_PRECISION, NUMERIC_SCALE),
            "TIMESTAMP" => DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            "DATETIME" => DataType::Timestamp(TimeUnit::Microsecond, None),
            "DATE" => DataType::Date32,
            "TIME" => DataType::Time64(TimeUnit::Microsecond),
            // CSV keeps BigQuery's base64 text; Parquet stores raw bytes.
            "BYTES" if fmt == DataFormat::Parquet => DataType::Binary,
            _ => DataType::Utf8,
        }
    }
}

fn arrow_schema(fields: &[BqField], fmt: DataFormat) -> Arc<Schema> {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|f| Field::new(&f.name, f.arrow_type(fmt), true))
            .collect::<Vec<_>>(),
    ))
}

/// Convert REST rows (``[{"f": [{"v": …}, …]}, …]``) into a record batch.
pub fn rows_to_batch(fields: &[BqField], rows: &[Value], fmt: DataFormat) -> Result<RecordBatch> {
    let columns = fields
        .iter()
        .enumerate()
        .map(|(idx, field)| {
            let cells = rows.iter().map(|row| cell(row, idx));
            build_column(field, field.arrow_type(fmt), cells)
                .with_context(|| format!("column '{}'", field.name))
        })
        .collect::<anyhow::Result<Vec<ArrayRef>>>()?;
    Ok(RecordBatch::try_new(arrow_schema(fields, fmt), columns)
        .context("failed to assemble record batch")?)
}

fn cell(row: &Value, idx: usize) -> &Value {
    row.get("f")
        .and_then(|f| f.get(idx))
        .and_then(|c| c.get("v"))
        .unwrap_or(&Value::Null)
}

fn text(v: &Value) -> Option<&str> {
    v.as_str()
}

fn build_column<'a>(
    field: &BqField,
    dtype: DataType,
    cells: impl Iterator<Item = &'a Value>,
) -> anyhow::Result<ArrayRef> {
    macro_rules! build {
        ($builder:expr, $parse:expr) => {{
            let mut b = $builder;
            for v in cells {
                match text(v) {
                    None => b.append_null(),
                    Some(s) => b.append_value($parse(s)?),
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }};
    }
    let array = match dtype {
        DataType::Utf8 if field.is_nested() => {
            let mut b = StringBuilder::new();
            for v in cells {
                if v.is_null() {
                    b.append_null();
                } else {
                    b.append_value(nested_to_json(field, v).to_string());
                }
            }
            Arc::new(b.finish()) as ArrayRef
        }
        DataType::Int64 => build!(Int64Builder::new(), |s: &str| s
            .parse::<i64>()
            .with_context(|| format!("invalid INT64 {s:?}"))),
        DataType::Float64 => build!(Float64Builder::new(), parse_float),
        DataType::Boolean => build!(BooleanBuilder::new(), |s: &str| anyhow::Ok(
            s.eq_ignore_ascii_case("true")
        )),
        DataType::Decimal128(p, sc) => build!(
            Decimal128Builder::new().with_precision_and_scale(p, sc)?,
            |s: &str| parse_decimal(s, sc as u32)
        ),
        DataType::Timestamp(_, Some(tz)) => build!(
            TimestampMicrosecondBuilder::new().with_timezone(tz),
            parse_timestamp_micros
        ),
        DataType::Timestamp(_, None) => {
            build!(TimestampMicrosecondBuilder::new(), parse_datetime_micros)
        }
        DataType::Date32 => build!(Date32Builder::new(), parse_date),
        DataType::Time64(_) => build!(Time64MicrosecondBuilder::new(), parse_time_micros),
        DataType::Binary => build!(BinaryBuilder::new(), |s: &str| B64
            .decode(s)
            .context("invalid base64")),
        _ => build!(StringBuilder::new(), |s: &str| anyhow::Ok(s.to_owned())),
    };
    Ok(array)
}

/// Turn a REST nested cell into natural JSON (objects keyed by field name).
fn nested_to_json(field: &BqField, v: &Value) -> Value {
    if field.mode == "REPEATED" {
        let item_field = BqField {
            mode: "NULLABLE".into(),
            ..field.clone()
        };
        return Value::Array(
            v.as_array()
                .map(|items| {
                    items
                        .iter()
                        .map(|item| {
                            nested_to_json(&item_field, item.get("v").unwrap_or(&Value::Null))
                        })
                        .collect()
                })
                .unwrap_or_default(),
        );
    }
    if matches!(field.field_type.as_str(), "RECORD" | "STRUCT") {
        let mut obj = Map::new();
        if let Some(cells) = v.get("f").and_then(Value::as_array) {
            for (sub, c) in field.fields.iter().zip(cells) {
                obj.insert(
                    sub.name.clone(),
                    nested_to_json(sub, c.get("v").unwrap_or(&Value::Null)),
                );
            }
        }
        return Value::Object(obj);
    }
    v.clone()
}

fn parse_float(s: &str) -> anyhow::Result<f64> {
    match s {
        "NaN" => Ok(f64::NAN),
        "Infinity" => Ok(f64::INFINITY),
        "-Infinity" => Ok(f64::NEG_INFINITY),
        _ => s.parse().with_context(|| format!("invalid FLOAT64 {s:?}")),
    }
}

/// Parse a plain decimal string into an integer scaled by ``10^scale``.
fn parse_decimal(s: &str, scale: u32) -> anyhow::Result<i128> {
    let (neg, digits) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (int_part, frac_part) = digits.split_once('.').unwrap_or((digits, ""));
    anyhow::ensure!(
        frac_part.len() <= scale as usize
            && !(int_part.is_empty() && frac_part.is_empty())
            && int_part
                .chars()
                .chain(frac_part.chars())
                .all(|c| c.is_ascii_digit()),
        "invalid NUMERIC {s:?}"
    );
    let padded = format!("{int_part}{frac_part:0<width$}", width = scale as usize);
    let value: i128 = padded
        .parse()
        .with_context(|| format!("invalid NUMERIC {s:?}"))?;
    Ok(if neg { -value } else { value })
}

/// TIMESTAMP values arrive as integer microseconds (``useInt64Timestamp``)
/// or, from older endpoints, as float seconds.
fn parse_timestamp_micros(s: &str) -> anyhow::Result<i64> {
    if let Ok(us) = s.parse::<i64>() {
        return Ok(us);
    }
    let secs: f64 = s
        .parse()
        .with_context(|| format!("invalid TIMESTAMP {s:?}"))?;
    Ok((secs * 1_000_000.0).round() as i64)
}

fn parse_datetime_micros(s: &str) -> anyhow::Result<i64> {
    let dt = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))
        .with_context(|| format!("invalid DATETIME {s:?}"))?;
    Ok(dt.and_utc().timestamp_micros())
}

fn parse_date(s: &str) -> anyhow::Result<i32> {
    let d =
        NaiveDate::parse_from_str(s, "%Y-%m-%d").with_context(|| format!("invalid DATE {s:?}"))?;
    Ok((d - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32)
}

fn parse_time_micros(s: &str) -> anyhow::Result<i64> {
    let t = NaiveTime::parse_from_str(s, "%H:%M:%S%.f")
        .with_context(|| format!("invalid TIME {s:?}"))?;
    Ok(t.num_seconds_from_midnight() as i64 * 1_000_000 + (t.nanosecond() / 1_000) as i64)
}

enum Sink {
    Csv(arrow_csv::Writer<std::fs::File>),
    Parquet(parquet::arrow::ArrowWriter<std::fs::File>),
}

/// Incremental CSV / Parquet writer fed with REST row pages, so a result
/// never has to be held in memory as a whole.
pub struct TableWriter {
    fields: Vec<BqField>,
    fmt: DataFormat,
    dest: PathBuf,
    sink: Sink,
}

/// File that receives the rows of *dest* until [`TableWriter::finish`]
/// renames it into place, so a failed write never replaces an existing
/// *dest*.
pub fn partial_path(dest: &Path) -> PathBuf {
    let mut name = dest.as_os_str().to_owned();
    name.push(".part");
    PathBuf::from(name)
}

impl TableWriter {
    /// Start writing rows of *fields* for *dest* (parent directories
    /// included).
    pub fn create(fields: Vec<BqField>, dest: &Path, fmt: DataFormat) -> Result<Self> {
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).map_err(|e| Error::io(parent, e))?;
        }
        let partial = partial_path(dest);
        let file = std::fs::File::create(&partial).map_err(|e| Error::io(&partial, e))?;
        let schema = arrow_schema(&fields, fmt);
        let sink = match fmt {
            DataFormat::Csv => {
                let mut writer = arrow_csv::WriterBuilder::new()
                    .with_header(true)
                    .build(file);
                // The header is emitted with the first batch, even an empty one.
                writer
                    .write(&RecordBatch::new_empty(schema))
                    .context("failed to write CSV")?;
                Sink::Csv(writer)
            }
            DataFormat::Parquet => {
                let props = parquet::file::properties::WriterProperties::builder()
                    .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
                    .build();
                Sink::Parquet(
                    parquet::arrow::ArrowWriter::try_new(file, schema, Some(props))
                        .context("failed to create Parquet writer")?,
                )
            }
        };
        Ok(TableWriter {
            fields,
            fmt,
            dest: dest.to_owned(),
            sink,
        })
    }

    /// Append one page of REST rows.
    pub fn write_rows(&mut self, rows: &[Value]) -> Result<()> {
        let batch = rows_to_batch(&self.fields, rows, self.fmt)?;
        match &mut self.sink {
            Sink::Csv(w) => w.write(&batch).context("failed to write CSV")?,
            Sink::Parquet(w) => w.write(&batch).context("failed to write Parquet")?,
        }
        Ok(())
    }

    /// Flush, close and move the file to its destination.
    pub fn finish(self) -> Result<()> {
        match self.sink {
            Sink::Csv(w) => drop(w),
            Sink::Parquet(w) => {
                w.close().context("failed to finish Parquet file")?;
            }
        }
        let partial = partial_path(&self.dest);
        std::fs::rename(&partial, &self.dest).map_err(|e| Error::io(&self.dest, e))
    }
}

/// Convert REST rows and write them to *dest*.
pub fn write_rows(fields: &[BqField], rows: &[Value], dest: &Path, fmt: DataFormat) -> Result<()> {
    let mut writer = TableWriter::create(fields.to_vec(), dest, fmt)?;
    writer.write_rows(rows)?;
    writer.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use serde_json::json;

    fn schema() -> Vec<BqField> {
        BqField::from_schema(Some(&json!({"fields": [
            {"name": "id", "type": "INTEGER"},
            {"name": "name", "type": "STRING"},
            {"name": "score", "type": "FLOAT"},
            {"name": "ok", "type": "BOOLEAN"},
            {"name": "amount", "type": "NUMERIC"},
            {"name": "ts", "type": "TIMESTAMP"},
            {"name": "d", "type": "DATE"},
            {"name": "tags", "type": "STRING", "mode": "REPEATED"},
            {"name": "rec", "type": "RECORD", "fields": [
                {"name": "a", "type": "INTEGER"}, {"name": "b", "type": "STRING"}
            ]}
        ]})))
    }

    fn rows() -> Vec<Value> {
        vec![
            json!({"f": [
                {"v": "1"}, {"v": "alice"}, {"v": "1.5"}, {"v": "true"}, {"v": "12.34"},
                {"v": "1735689600000000"}, {"v": "2025-01-01"},
                {"v": [{"v": "x"}, {"v": "y"}]},
                {"v": {"f": [{"v": "7"}, {"v": "z"}]}}
            ]}),
            json!({"f": [
                {"v": "2"}, {"v": null}, {"v": null}, {"v": "false"}, {"v": "-0.5"},
                {"v": null}, {"v": null}, {"v": []}, {"v": null}
            ]}),
        ]
    }

    #[test]
    fn converts_types() {
        let batch = rows_to_batch(&schema(), &rows(), DataFormat::Parquet).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.column(0).data_type(), &DataType::Int64);
        assert!(batch.column(1).is_null(1));
        let tags = batch
            .column(7)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(tags.value(0), r#"["x","y"]"#);
        let rec = batch
            .column(8)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(rec.value(0), r#"{"a":"7","b":"z"}"#);
    }

    #[test]
    fn decimals() {
        assert_eq!(parse_decimal("12.34", 9).unwrap(), 12_340_000_000);
        assert_eq!(parse_decimal("-0.5", 9).unwrap(), -500_000_000);
        assert_eq!(parse_decimal("7", 9).unwrap(), 7_000_000_000);
        assert!(parse_decimal("1e5", 9).is_err());
    }

    #[test]
    fn writes_csv_and_parquet() {
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("data/out.csv");
        write_rows(&schema(), &rows(), &csv, DataFormat::Csv).unwrap();
        let text = std::fs::read_to_string(&csv).unwrap();
        let mut lines = text.lines();
        assert_eq!(
            lines.next().unwrap(),
            "id,name,score,ok,amount,ts,d,tags,rec"
        );
        assert!(
            lines
                .next()
                .unwrap()
                .starts_with("1,alice,1.5,true,12.340000000,2025-01-01T00:00:00")
        );

        let pq = dir.path().join("data/out.parquet");
        write_rows(&schema(), &rows(), &pq, DataFormat::Parquet).unwrap();
        let file = std::fs::File::open(&pq).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let total: usize = reader.map(|b| b.unwrap().num_rows()).sum();
        assert_eq!(total, 2);
    }

    #[test]
    fn pages_are_appended_to_one_file() {
        let dir = tempfile::tempdir().unwrap();
        for (fmt, name) in [
            (DataFormat::Csv, "p.csv"),
            (DataFormat::Parquet, "p.parquet"),
        ] {
            let dest = dir.path().join(name);
            let mut writer = TableWriter::create(schema(), &dest, fmt).unwrap();
            writer.write_rows(&rows()[..1]).unwrap();
            writer.write_rows(&rows()[1..]).unwrap();
            writer.finish().unwrap();
            let total = match fmt {
                DataFormat::Csv => {
                    let text = std::fs::read_to_string(&dest).unwrap();
                    assert_eq!(text.matches("id,name").count(), 1);
                    text.lines().count() - 1
                }
                DataFormat::Parquet => {
                    let file = std::fs::File::open(&dest).unwrap();
                    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                        .unwrap()
                        .build()
                        .unwrap()
                        .map(|b| b.unwrap().num_rows())
                        .sum()
                }
            };
            assert_eq!(total, 2);
        }
    }

    #[test]
    fn unfinished_write_keeps_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("out.csv");
        std::fs::write(&dest, "old").unwrap();
        let mut writer = TableWriter::create(schema(), &dest, DataFormat::Csv).unwrap();
        writer.write_rows(&rows()).unwrap();
        drop(writer);
        assert_eq!(std::fs::read_to_string(&dest).unwrap(), "old");
        assert!(partial_path(&dest).exists());
    }

    #[test]
    fn empty_result() {
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("empty.csv");
        write_rows(&schema()[..2], &[], &csv, DataFormat::Csv).unwrap();
        assert_eq!(std::fs::read_to_string(&csv).unwrap().trim(), "id,name");
    }
}
