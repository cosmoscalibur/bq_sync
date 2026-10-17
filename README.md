# bq-sync

Sync BigQuery resources to a local directory structure.

## Install

`bq-sync` is written in Rust and ships both as a native binary and as a
Python package (PyO3 extension, one abi3 wheel for CPython ≥ 3.12).

```zsh
# Python package (provides the `bq-sync` command and `import bq_sync`)
pip install .
# or
uv pip install .

# Native binary only
cargo install --path .
```

Building from source requires Rust 1.99 (pinned in `rust-toolchain.toml`)
and a C compiler (tree-sitter grammar).

### Google Cloud access

All Google Cloud calls go through the official Google Cloud Rust SDK
([`google-cloud-rust`](https://github.com/googleapis/google-cloud-rust)):
`google-cloud-bigquery-v2` (tables, routines, query jobs),
`google-cloud-bigquery-datatransfer-v1` (scheduled queries) and
`google-cloud-dataform-v1` (saved queries). Two BigQuery endpoints the SDK
does not expose — `tabledata.list` (row reads for `fetch`) and the media
upload behind `push --data` — are called over HTTPS with the SDK's
credentials. Row pages are written to the output file as they arrive and
`push --data` streams the source file.

Credentials come from Application Default Credentials via
`google-cloud-auth`: `GOOGLE_APPLICATION_CREDENTIALS`,
`gcloud auth application-default login`, or the GCE/GKE metadata server.
They are only needed by commands that call Google Cloud (`check` works
offline unless `--online` is given).

## Quick Start

1. Copy the template config to your project root:

   ```zsh
   cp examples/bq_sync.toml /path/to/your-project/bq_sync.toml
   ```

2. Edit `bq_sync.toml` with your GCP project ID, region, and datasets.

3. Run the sync:

   ```zsh
   bq-sync pull
   ```

## CLI Usage

```text
bq-sync [--version] [-v]
bq-sync pull        [--dataset DATASET] [--dry-run] [--config PATH] [--force] [--force-file FILE]
bq-sync fetch       <project/dataset/model> [-f csv|parquet] [-o DIR] [--config PATH]
bq-sync push        [FILE ...] [--data SOURCE DEST] [--since HOURS] [--dry-run] [--yes] [--include-models] [--config PATH]
bq-sync rm          <path>... [--dry-run] [--yes] [--config PATH]
bq-sync check       [FILE ...] [--online] [--config PATH]
bq-sync materialize <project/dataset/name> [--target NAME] [--dry-run] [--yes] [--config PATH]
bq-sync reschedule  [DISPLAY_NAME] [--schedule S | --from-file] [--dry-run] [--trigger] [--list] [--config PATH]
```

### pull options

| Flag | Description |
| --- | --- |
| `--config` | Path to `bq_sync.toml` (default: auto-discover from CWD upward) |
| `--dataset` | Sync a single dataset (default: all configured) |
| `--dry-run` | Preview actions without writing files |
| `--force` | Force fetch all files, bypassing decision matrix |
| `--force-file FILE` | Force fetch a specific file (repeatable) |
| `-v`, `--verbose` | Enable DEBUG logging |

The resources of a dataset and the project-level queries are listed
concurrently; datasets are processed one after another. Scheduled and saved query names are free text: path separators
in a name become `_` in the file name (cut to 200 bytes), and a name that maps
to a file already written in the same pull, ignoring case, is skipped with a
warning, as is a name containing a line break. The original name is kept in
the `-- Scheduled Query:` / `-- Saved Query:` header.

### fetch options

| Flag | Description |
| --- | --- |
| `model` (positional) | BigQuery resource path: `<project>/<dataset>/<model>` or `<project>/<dataset>/<resource_type>/<model>` |
| `-f`, `--format` | Output format: `csv` (default) or `parquet` |
| `-o`, `--output-dir` | Directory where a `data/` folder is created (default: config output dir) |
| `--config` | Path to `bq_sync.toml` (default: auto-discover from CWD upward) |

Native tables are read with `tabledata.list`; views, external tables and
any other table type are read by running `SELECT *` on them. Scalar columns keep their
BigQuery type (INT64, FLOAT64, BOOL, NUMERIC, TIMESTAMP, DATETIME, DATE,
TIME, BYTES); `REPEATED` and `RECORD` columns are written as JSON text.
Parquet output is ZSTD-compressed.

### push options

If no positional file paths or `--data` is provided, **auto mode** is used:
detect changed files via `git status` (preferred) or by file modification time
when `--since` is explicitly set.  Both modes print the changeset and prompt for
`y/N` confirmation.

By default, auto mode excludes materialized model YAMLs (tables without a
corresponding SQL file).  Use `--include-models` to include them.

Pushable resources: views (SQL), routines (SQL/JS), models (YAML descriptions),
saved queries (SQL) and scheduled queries (SQL).

Scheduled queries are update-only: `push` replaces the query SQL of an
existing scheduled query (matched by its `-- Scheduled Query:` header) and
keeps its other settings. The `-- Schedule:` header is not pushed — use
`bq-sync reschedule --from-file` for schedule changes. A scheduled query
that does not exist in BigQuery is skipped with a warning; create it in the
console first. Scheduled query display names are not unique in BigQuery: a name
shared by several scheduled queries makes `push` and `reschedule` fail for
that name.

| Flag | Description |
| --- | --- |
| `FILE` (positional) | Manual mode: files to push (one or more) |
| `--data SOURCE DEST` | Table-replace: local CSV/Parquet path + `project/dataset/table` (manual only) |
| `--since HOURS` | Auto mode: use mtime-based detection with the given look-back window (skips git) |
| `--include-models` | Auto mode: include materialized model YAMLs (excluded by default) |
| `--dry-run` | Preview changeset without writing to BQ |
| `-y`, `--yes` | Skip interactive confirmation |
| `--config` | Path to `bq_sync.toml` (default: auto-discover from CWD upward) |

### rm options

Deletes the BQ resource first, then removes the local file on success.
Scheduled query files are skipped with a warning.

| Flag | Description |
| --- | --- |
| `path` (positional) | Local file paths identifying BQ resources to delete (repeatable) |
| `--dry-run` | Preview without deleting |
| `-y`, `--yes` | Skip interactive confirmation |
| `--config` | Path to `bq_sync.toml` (default: auto-discover from CWD upward) |

## Configuration

```toml
[project]
id = "your-gcp-project-id"
default_region = "us-east1"

[sync]
datasets = ["dataset_name"]
output_dir = "."  # Relative to this config file
```

## Output Directory Structure

```text
<output_dir>/
└── <project_id>/
    ├── <dataset>/
    │   ├── views/
    │   ├── routines/
    │   ├── models/
    │   └── externals/
    ├── data/
    ├── scheduled_queries/
    └── saved_queries/
```

## Fetch Decision

Before any sync, uncommitted tracked changes in the output directory
cause `bq-sync` to **warn and exit**. All files must be committed first.

| BQ exists? | File exists? | Git history? | Condition | Action |
| --- | --- | --- | --- | --- |
| Yes | No | — | — | Fetch |
| Yes | Yes | No | — | Warn (pending commit) |
| Yes | Yes | Yes | BQ ≤ git | Skip |
| Yes | Yes | Yes | BQ > git | Fetch |
| No | No | — | — | Skip |
| No | Yes | Yes | — | Warn |

`--force` bypasses the decision matrix entirely. Git history serves as backup.

Saved queries are read and written through the Dataform API as the
`content.sql` file of the first workspace of the repository whose display
name matches the saved query.

## Python API

The Python package exposes the same operations as functions:

```python
import bq_sync

summary = bq_sync.check(config="bq_sync.toml")      # offline SQL/JS validation
print(summary["failed"], summary["warned"], summary["passed"])

bq_sync.pull(dry_run=True)                           # same as `bq-sync pull --dry-run`
bq_sync.fetch("my-project/my_dataset/my_table", format="parquet", output_dir="data")
bq_sync.read_model_yaml("my-project/my_dataset/models/users.yaml")
```

Failures raise `bq_sync.BqSyncError` (`bq_sync.NotFoundError` for missing
BigQuery resources, `OSError` for filesystem errors). Long-running calls
release the GIL. Type stubs are included (`bq_sync/_bq_sync.pyi`).

## Development

```zsh
cargo test                      # Rust unit tests
cargo clippy --all-targets --features python
uv venv && uv pip install maturin pytest
maturin develop --uv            # build the extension into the venv
pytest                          # Python binding tests
```

Layout: `src/` (Rust crate: library, `bq-sync` binary, PyO3 module in
`src/python.rs` behind the `python` feature), `python/bq_sync/` (Python
package shim and stubs), `tests/` (Python binding tests).

## License

MIT
