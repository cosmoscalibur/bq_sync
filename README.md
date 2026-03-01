# bq-sync

Sync BigQuery resources to a local directory structure.

## Install

Requires Python ≥ 3.13.

```zsh
pip install .
# or
uv pip install .
```

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

```
bq-sync pull  [--dataset DATASET] [--dry-run] [--config PATH] [--force] [--force-file FILE]
bq-sync fetch <project/dataset/model> [-f csv|parquet] [-o DIR] [--config PATH]
bq-sync push  [--path FILE]... [--data SOURCE DEST] [--since HOURS] [--dry-run] [--yes] [--config PATH]
bq-sync rm    <path>... [--dry-run] [--yes] [--config PATH]
```

### pull options

| Flag | Description |
|---|---|
| `--config` | Path to `bq_sync.toml` (default: auto-discover from CWD upward) |
| `--dataset` | Sync a single dataset (default: all configured) |
| `--dry-run` | Preview actions without writing files |
| `--force` | Force fetch all files, bypassing decision matrix |
| `--force-file FILE` | Force fetch a specific file (repeatable) |
| `-v`, `--verbose` | Enable DEBUG logging |

### fetch options

| Flag | Description |
|---|---|
| `model` (positional) | BigQuery resource path: `<project>/<dataset>/<model>` or `<project>/<dataset>/<resource_type>/<model>` |
| `-f`, `--format` | Output format: `csv` (default) or `parquet` |
| `-o`, `--output-dir` | Directory where a `data/` folder is created (default: config output dir) |
| `--config` | Path to `bq_sync.toml` (default: auto-discover from CWD upward) |

### push options

If neither `--path` nor `--data` is provided, **auto mode** is used: detect
changed files via `git status` (preferred) or by file modification time
(`--since`).  Both modes print the changeset and prompt for `y/N` confirmation.

Pushable resources: views (SQL), routines (SQL/JS), models (YAML descriptions),
and saved queries (SQL).

| Flag | Description |
|---|---|
| `--path FILE` | Manual mode: push a specific file (repeatable) |
| `--data SOURCE DEST` | Table-replace: local CSV/Parquet path + `project/dataset/table` (manual only) |
| `--since HOURS` | Fallback look-back window when git is unavailable (default: 24) |
| `--dry-run` | Preview changeset without writing to BQ |
| `-y`, `--yes` | Skip interactive confirmation |
| `--config` | Path to `bq_sync.toml` (default: auto-discover from CWD upward) |

### rm options

Deletes the BQ resource first, then removes the local file on success.

| Flag | Description |
|---|---|
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

```
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
|---|---|---|---|---|
| Yes | No | — | — | Fetch |
| Yes | Yes | No | — | Warn (pending commit) |
| Yes | Yes | Yes | BQ ≤ git | Skip |
| Yes | Yes | Yes | BQ > git | Fetch |
| No | No | — | — | Skip |
| No | Yes | Yes | — | Warn |

`--force` bypasses the decision matrix entirely. Git history serves as backup.

> **Warning**
> Saved queries use the Dataform API, which is relatively new and may
> change without notice.

## License

MIT
