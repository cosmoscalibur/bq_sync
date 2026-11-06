# bq-sync

Sync BigQuery resources to a local directory structure.

## Install

`bq-sync` is written in Rust and ships both as a native binary and as a
Python package (PyO3 extension, one abi3 wheel for CPython ≥ 3.12).

### From a GitHub release

Each release (`vX.Y.Z`) attaches the CLI binaries
(`bq-sync-vX.Y.Z-<target>.tar.gz` / `.zip` for Linux x86_64 and arm64,
macOS arm64 and Windows x86_64), Python wheels (abi3, CPython ≥ 3.12) for the
same platforms, an sdist and `SHA256SUMS`.

`--find-links` points uv at the release's asset list and it picks the wheel
matching your platform; no Rust toolchain is needed.

Run it without installing (`uvx`):

```zsh
uvx --find-links https://github.com/cosmoscalibur/bq_sync/releases/expanded_assets/v0.3.2 \
  bq-sync --help
```

Install it as a uv tool:

```zsh
uv tool install bq-sync \
  --find-links https://github.com/cosmoscalibur/bq_sync/releases/expanded_assets/v0.3.2
```

Use it as a library in a project (`import bq_sync`):

```zsh
uv add bq-sync \
  --find-links https://github.com/cosmoscalibur/bq_sync/releases/expanded_assets/v0.3.2
```

or install a specific wheel by URL (copy it from the release page):

```zsh
uv tool install https://github.com/cosmoscalibur/bq_sync/releases/download/v0.3.2/<wheel-file>.whl
```

To upgrade, rerun the command with the new tag.
To use the standalone binary instead, download the archive for your platform
from the release, verify it against `SHA256SUMS`, extract it and put `bq-sync`
on your `PATH`.

### From source

```zsh
# Python package (provides the `bq-sync` command and `import bq_sync`)
uv tool install .
# or
pip install .

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

`push` makes BigQuery match the file, whether the scheduled query is new or
not (matched by its `-- Scheduled Query:` header). An existing query gets its
SQL replaced and, if the `-- Schedule:` header differs, its schedule updated;
other settings are kept. (`bq-sync reschedule` remains for schedule-only
changes.) A query that does not exist in BigQuery is created from the
`-- Scheduled Query:` and `-- Schedule:` headers and the SQL body (a missing
`-- Schedule:` header is an error). A created query has no destination dataset, so its SQL must be
a DDL/DML script (e.g. `CREATE OR REPLACE TABLE ...`); other settings
(service account, notifications, destination table) are not set. Scheduled query display names are not unique in BigQuery: a name
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

### materialize

Creates `<dataset>.<target>` from a view or external table and refreshes the
model YAML. The resource can be given by either name of the pair: `view_x` /
`external_x` (target `materialize_x`) or `materialize_x` itself (the source is
the existing `view_x` or `external_x`).

If a scheduled query named after the target exists (the `-- Scheduled Query:`
header of its file, e.g. `materialize_x`), `materialize` runs that scheduled
query and waits for it instead of the generic
`CREATE OR REPLACE TABLE … AS SELECT *`, so customizations such as `CLUSTER BY`
or partitioning are kept. Without it, the generic DDL runs.

### rm options

Deletes the BQ resource first, then removes the local file on success.
A scheduled query is resolved by the name in its `-- Scheduled Query:` header
(several scheduled queries with the same name are an error). Its settings other
than SQL and schedule are not stored in git, so the deletion cannot be undone
from the repository; without `--yes` the confirmation prompt applies. If the
scheduled query no longer exists in BigQuery, only the local file is removed.

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
name matches the saved query. A saved query created by `push` gets the label
`single-file-asset-type: sql` (what BigQuery Studio uses to list it), and every
`push` of a saved query commits and pushes the workspace with the message
`Saved`, so Studio finds its history. The commit author is the git identity
(`git config user.name` / `user.email`), or the active gcloud account when git
has none, with the tool appended: `Edward Villegas (bq-sync)`. Saved queries left uncommitted by
earlier versions are committed the next time they are pushed by path
(`bq-sync push saved_queries/<name>.sql`), even if their content did not
change.

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
