# Changelog

All notable changes to this project are documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Added

- `push` updates the query SQL of existing scheduled queries from
  `scheduled_queries/*.sql`, keeping their other settings.

### Changed

- bq-sync is implemented in Rust. The CLI and configuration format are
  unchanged; the Python package exposes the same operations through the
  `bq_sync._bq_sync` PyO3 extension and requires CPython 3.12 or newer.
- `fetch` writes rows page by page and `push --data` streams the source
  file, so table size is not bounded by memory.
- A scheduled query display name shared by several scheduled queries is an
  error in `push` and `reschedule` instead of matching the first one.
- `rm` skips scheduled query files.
- `pull` replaces path separators in scheduled and saved query file names
  and skips names that collide or contain a line break.
- Listing saved queries returns an empty list only when Dataform is not
  enabled or not accessible; other errors are reported.
