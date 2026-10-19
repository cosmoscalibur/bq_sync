# Changelog

All notable changes to this project are documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Added

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
