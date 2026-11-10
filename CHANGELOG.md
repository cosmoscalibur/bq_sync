# Changelog

All notable changes to this project are documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

## [0.3.3] - 2026-11-10

### Added

- `ci.yml` workflow: `cargo fmt --check`, `cargo clippy -D warnings`,
  `cargo test` and the Python binding tests on pull requests and pushes to
  `main` (#19).
- `pull --refresh-metadata` (`refresh_metadata` in the Python binding) to
  rewrite the volatile model metadata on demand (#21).

### Changed

- `pull` no longer rewrites a model YAML whose only differences are the
  volatile metadata (`row_count`, `created`, `modified`,
  `total_logical_bytes`), avoiding metadata-only diffs (#21).
- Commits made in BigQuery use the active `gcloud` account as author email;
  the git email is only a fallback (#24).

### Fixed

- `pull` creates the model YAML of a view or routine whose local SQL is
  already up to date, e.g. one created by `push` (#22).
- `rm` of a saved query deletes the repository's workspaces first, instead
  of failing with HTTP 400 (#23).

## [0.3.2] - 2026-11-06

### Added

- `push` creates a scheduled query that does not exist in BigQuery from the
  `-- Scheduled Query:` / `-- Schedule:` headers and the SQL body (no
  destination dataset, so DDL/DML scripts). Previously it was skipped with
  a warning.
- `push` of an existing scheduled query also updates its schedule when the
  `-- Schedule:` header differs (previously only the SQL was pushed).

### Fixed

- `check` no longer aborts with a `PanicException` on tables with
  `STRUCT`/`RECORD`/`ARRAY` columns (e.g. Datastream's `datastream_metadata`).
  Columns whose type `inbq` cannot represent are left out of the local
  catalog, duplicate column names are dropped, and any remaining `inbq` panic
  is reported as a lineage warning (#8).

## [0.3.1] - 2026-11-06

### Added

- `rm` deletes scheduled queries (Data Transfer transfer configs) resolved
  by their `-- Scheduled Query:` header name, then removes the local file.
  If the scheduled query no longer exists remotely, only the local file is
  removed.
- `materialize` accepts either name of the view/materialize pair
  (`view_x` / `external_x` or `materialize_x`). If a scheduled query named
  after the target exists, it is run (and awaited) instead of the generic
  `SELECT *` DDL, preserving its customization (`CLUSTER BY`, partitioning,
  options); then the model YAML is refreshed. Without it the behavior is
  unchanged.

### Fixed

- `push` creates new saved queries with the `single-file-asset-type: sql`
  label, so BigQuery Studio lists them.
- `push` commits and pushes the Dataform workspace when creating or
  updating a saved query, so Studio no longer fails with "no commits found".
  Changes left uncommitted by earlier versions are committed on the next
  push of that file. The commit author is the git user (or the active gcloud
  account) followed by `(bq-sync)`, e.g. `Edward Villegas (bq-sync)`.

## [0.3.0] - 2026-11-05

### Added

- Release workflow: pushing a `vX.Y.Z` tag builds CLI binaries and PyO3
  wheels (Linux x86_64/arm64, macOS arm64, Windows x86_64) plus an sdist and
  checksums, and publishes them as a GitHub release.
- `push` updates the query SQL of existing scheduled queries from
  `scheduled_queries/*.sql`, keeping their other settings. `rm` skips
  scheduled query files.

### Changed

- bq-sync is implemented in Rust. The CLI and configuration format are
  unchanged; the Python package exposes the same operations through the
  `bq_sync._bq_sync` PyO3 extension and requires CPython 3.12 or newer.
- `fetch` writes rows page by page and `push --data` streams the source
  file instead of loading either in memory.
- A scheduled query display name shared by several scheduled queries is an
  error in `push` and `reschedule`; `reschedule` previously used the first
  match.
- `pull` replaces path separators in scheduled and saved query file names
  and skips names that collide or contain a line break.
- Listing saved queries returns an empty list only when Dataform is not
  enabled or not accessible; other errors are reported.
