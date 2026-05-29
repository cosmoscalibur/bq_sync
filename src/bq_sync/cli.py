"""CLI entrypoint for bq-sync."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from bq_sync import __version__
from bq_sync.config import SyncConfig, discover_config, load_config, resolve_output_dir
from bq_sync.pull import pull_project


def _build_parser() -> argparse.ArgumentParser:
    """Build the argument parser with subcommands."""
    parser = argparse.ArgumentParser(
        prog="bq-sync",
        description="Sync BigQuery resources to a local directory.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG) logging.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- pull ---
    pull_parser = subparsers.add_parser(
        "pull",
        help="Fetch BQ resources to local files.",
    )
    pull_parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to bq_sync.toml (default: auto-discover from CWD).",
    )
    pull_parser.add_argument(
        "--dataset",
        type=str,
        default=None,
        help="Sync a single dataset (default: all configured).",
    )
    pull_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview actions without writing files.",
    )
    pull_parser.add_argument(
        "--force",
        action="store_true",
        help="Force fetch all files, bypassing decision matrix.",
    )
    pull_parser.add_argument(
        "--force-file",
        type=str,
        action="append",
        default=None,
        help="Force fetch a specific file (repeatable).",
    )

    # --- push ---
    push_parser = subparsers.add_parser(
        "push",
        help="Deploy local resources to BigQuery.",
    )
    push_parser.add_argument(
        "paths",
        nargs="*",
        default=[],
        help="Manual mode: files to push (positional).",
    )
    push_parser.add_argument(
        "--data",
        nargs=2,
        metavar=("SOURCE", "DEST"),
        default=None,
        help=(
            "Table-replace: local CSV/Parquet path and "
            "project/dataset/table destination (manual only)."
        ),
    )
    push_parser.add_argument(
        "--since",
        type=float,
        default=None,
        help=(
            "Auto mode: use mtime-based detection with the given "
            "look-back window in hours (skips git detection)."
        ),
    )
    push_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changeset without executing writes.",
    )
    push_parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="Skip interactive confirmation prompt.",
    )
    push_parser.add_argument(
        "--include-models",
        action="store_true",
        help=("Auto mode: include materialized model YAMLs (excluded by default)."),
    )
    push_parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to bq_sync.toml (default: auto-discover from CWD).",
    )

    # --- fetch ---
    fetch_parser = subparsers.add_parser(
        "fetch",
        help="Download table/view data as CSV or Parquet.",
    )
    fetch_parser.add_argument(
        "model",
        type=str,
        help="BigQuery resource path: <project>/<dataset>/<model>.",
    )
    fetch_parser.add_argument(
        "-f",
        "--format",
        type=str,
        choices=["csv", "parquet"],
        default="csv",
        help="Output format (default: csv).",
    )
    fetch_parser.add_argument(
        "-o",
        "--output-dir",
        type=str,
        default=None,
        help="Directory for output files (used as-is; default: config data dir).",
    )
    fetch_parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to bq_sync.toml (default: auto-discover from CWD).",
    )

    # --- rm ---
    rm_parser = subparsers.add_parser(
        "rm",
        help="Delete BQ resources and their local files.",
    )
    rm_parser.add_argument(
        "path",
        nargs="+",
        help="Local file paths identifying BQ resources to delete.",
    )
    rm_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview without deleting.",
    )
    rm_parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="Skip interactive confirmation.",
    )
    rm_parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to bq_sync.toml (default: auto-discover from CWD).",
    )

    # --- check ---
    check_parser = subparsers.add_parser(
        "check",
        help="Validate SQL syntax, references, and column lineage.",
    )
    check_parser.add_argument(
        "paths",
        nargs="*",
        default=[],
        help="SQL files to validate (default: auto-discover all).",
    )
    check_parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to bq_sync.toml (default: auto-discover from CWD).",
    )

    return parser


def main(argv: list[str] | None = None) -> None:
    """CLI entrypoint.

    Args:
        argv: Command-line arguments.  Defaults to ``sys.argv[1:]``.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)

    level = logging.DEBUG if args.verbose else logging.INFO
    fmt = "%(levelname)s: %(message)s" if args.verbose else "%(message)s"
    logging.basicConfig(level=level, format=fmt)

    if args.command == "push":
        _handle_push(args)
    elif args.command == "pull":
        _handle_pull(args)
    elif args.command == "fetch":
        _handle_fetch(args)
    elif args.command == "rm":
        _handle_rm(args)
    elif args.command == "check":
        _handle_check(args)


def _resolve_config(args: argparse.Namespace) -> tuple[Path, SyncConfig]:
    """Discover and load config from CLI args.

    Args:
        args: Parsed CLI namespace (must have a ``config`` attribute).

    Returns:
        Tuple of (config_path, SyncConfig).
    """
    if args.config:
        config_path = Path(args.config).resolve()
    else:
        try:
            config_path = discover_config()
        except FileNotFoundError as exc:
            logging.error("%s", exc)
            sys.exit(1)

    return config_path, load_config(config_path)


def _handle_pull(args: argparse.Namespace) -> None:
    """Handle the ``pull`` subcommand."""
    config_path, config = _resolve_config(args)

    # If --dataset narrows the scope, override config.
    if args.dataset:
        config = config.__class__(
            project=config.project,
            datasets=[args.dataset],
            output_dir=config.output_dir,
        )

    pull_project(
        config,
        config_path,
        dry_run=args.dry_run,
        force=args.force,
        force_files=args.force_file,
    )


def _handle_push(args: argparse.Namespace) -> None:
    """Handle the ``push`` subcommand."""
    from bq_sync.push import DataSpec, push_auto, push_manual

    config_path, config = _resolve_config(args)

    # Build DataSpec from --data if provided.
    data_spec: DataSpec | None = None
    if args.data:
        source_str, dest_str = args.data
        dest_parts = dest_str.split("/")
        if len(dest_parts) != 3:
            logging.error(
                "Invalid --data destination '%s': expected "
                "project/dataset/table "
                "(e.g. 'my-project/my_dataset/my_table').",
                dest_str,
            )
            sys.exit(1)
        source_path = Path(source_str).resolve()
        fmt = "parquet" if source_path.suffix == ".parquet" else "csv"
        data_spec = DataSpec(
            source=source_path,
            project=dest_parts[0],
            dataset=dest_parts[1],
            table=dest_parts[2],
            fmt=fmt,
        )

    is_manual = args.paths or args.data

    if is_manual:
        push_manual(
            config,
            config_path,
            paths=args.paths,
            dry_run=args.dry_run,
            yes=args.yes,
            data_spec=data_spec,
        )
    else:
        use_mtime = args.since is not None
        since_hours = args.since if use_mtime else 24.0
        push_auto(
            config,
            config_path,
            dry_run=args.dry_run,
            yes=args.yes,
            since_hours=since_hours,
            include_models=args.include_models,
            use_mtime=use_mtime,
        )


def _handle_rm(args: argparse.Namespace) -> None:
    """Handle the ``rm`` subcommand."""
    from bq_sync.push import rm_resources

    config_path, config = _resolve_config(args)
    rm_resources(
        config,
        config_path,
        paths=args.path,
        dry_run=args.dry_run,
        yes=args.yes,
    )


def _resolve_fetch_data_dir(args: argparse.Namespace) -> Path:
    """Resolve the data output directory for the ``fetch`` subcommand.

    When ``--output-dir`` is given, the path is used as-is.
    Otherwise the default ``<project>/data`` path is used via config.

    Args:
        args: Parsed CLI namespace.

    Returns:
        Absolute path to the data output directory.
    """
    if args.output_dir:
        return Path(args.output_dir).resolve()
    config_path, config = _resolve_config(args)
    return resolve_output_dir(config, config_path) / "data"


def _resolve_sql_file(args: argparse.Namespace, resource_type: str, name: str) -> Path:
    """Build the expected local SQL file path from the pull output structure.

    Args:
        args: Parsed CLI namespace (used for config resolution).
        resource_type: Resource subdirectory (``"views"``, ``"saved_queries"``).
        name: Resource name (without extension).

    Returns:
        Absolute path where the SQL file is expected.
    """
    config_path, config = _resolve_config(args)
    output_root = resolve_output_dir(config, config_path)
    return output_root / resource_type / f"{name}.sql"


def _handle_fetch(args: argparse.Namespace) -> None:
    """Handle the ``fetch`` subcommand.

    Resolution strategy:

    - Path ending in ``.sql``: validate the local file exists.  If it
      does, execute its SQL via ``fetch_query_to_file``.  If not, fall
      back to resource-type resolution.
    - Path **not** ending in ``.sql``: use resource-type resolution
      directly (``list_rows`` for tables/views, Dataform for saved
      queries).
    """
    from bq_sync import bq_client

    fmt: str = args.format
    data_dir = _resolve_fetch_data_dir(args)
    parts = args.model.split("/")
    ends_with_sql = parts[-1].endswith(".sql")

    # --- saved queries: <project>/saved_queries/<name>[.sql] ---
    if len(parts) == 3 and parts[1] == "saved_queries":
        project = parts[0]
        name = Path(parts[2]).stem
        dest = data_dir / f"{name}.{fmt}"

        if ends_with_sql:
            sql_path = _resolve_sql_file(args, "saved_queries", name)
            if sql_path.is_file():
                sql = sql_path.read_text(encoding="utf-8")
                logging.info("Executing local SQL %s -> %s", sql_path, dest)
                bq_client.fetch_query_to_file(project, sql, dest, fmt=fmt)
                logging.info("Saved %s", dest)
                return

        # Resource-type resolution: Dataform API lookup.
        config_path, config = _resolve_config(args)
        region = config.project.default_region
        saved_list = bq_client.list_saved_queries(project, region)
        match = next((s for s in saved_list if s.name == name), None)
        if match is None:
            logging.error(
                "Saved query '%s' not found in project '%s' region '%s'.",
                name,
                project,
                region,
            )
            sys.exit(1)
        logging.info("Executing saved query '%s' -> %s", name, dest)
        bq_client.fetch_query_to_file(project, match.sql, dest, fmt=fmt)
        logging.info("Saved %s", dest)
        return

    # --- tables/views: <project>/<dataset>[/<resource_type>]/<name> ---
    if len(parts) == 4:
        project, dataset, resource_type, name = parts
    elif len(parts) == 3:
        project, dataset, name = parts
        resource_type = None
    else:
        logging.error(
            "Invalid model path '%s': expected "
            "<project>/<dataset>/<table_or_view>, "
            "<project>/<dataset>/<resource_type>/<table_or_view>, or "
            "<project>/saved_queries/<name> "
            "but got %d segments. "
            "Example: my-project/my_dataset/my_view",
            args.model,
            len(parts),
        )
        sys.exit(1)

    model = Path(name).stem
    dest = data_dir / f"{model}.{fmt}"

    # .sql extension triggers local file resolution.
    if ends_with_sql and resource_type:
        sql_path = _resolve_sql_file(args, f"{dataset}/{resource_type}", model)
        if sql_path.is_file():
            sql = sql_path.read_text(encoding="utf-8")
            logging.info("Executing local SQL %s -> %s", sql_path, dest)
            bq_client.fetch_query_to_file(project, sql, dest, fmt=fmt)
            logging.info("Saved %s", dest)
            return

    # Resource-type resolution: BQ list_rows.
    logging.info("Fetching %s -> %s", args.model, dest)
    bq_client.fetch_table_to_file(project, dataset, model, dest, fmt=fmt)
    logging.info("Saved %s", dest)


def _handle_check(args: argparse.Namespace) -> None:
    """Handle the ``check`` subcommand.

    Validates SQL files for syntax errors, unresolved table references,
    and column lineage issues using ``inbq``.
    """
    from bq_sync.sql_check import (
        CheckSummary,
        check_files,
        discover_sql_files,
    )

    config_path, config = _resolve_config(args)
    output_root = resolve_output_dir(config, config_path)
    project = config.project.id

    # Determine files to check.
    if args.paths:
        files = [Path(p).resolve() for p in args.paths]
    else:
        files = discover_sql_files(output_root)

    if not files:
        logging.info("No SQL files to validate.")
        return

    logging.info("Validating %d SQL file(s)...\n", len(files))

    summary: CheckSummary = check_files(files, output_root, project)

    # Pretty-print results.
    for result in summary.results:
        rel = result.path.name
        try:
            rel = str(result.path.relative_to(output_root))
        except ValueError:
            pass

        if result.level == "error":
            icon = "\u2717"  # ✗
        elif result.level == "warning":
            icon = "\u2713"  # ✓ (with warnings below)
        else:
            icon = "\u2713"  # ✓

        print(f"  {icon} {rel}")

        for table in result.tables_resolved:
            print(f"      Tables: {table} \u2713")
        for err in result.errors:
            print(f"      {err}")
        for warn in result.warnings:
            print(f"      \u26a0 {warn}")
        for info_msg in result.info:
            print(f"      \u24d8 {info_msg}")

    # Summary line.
    print(
        f"\n  {summary.failed} error(s), "
        f"{summary.warned} warning(s), "
        f"{summary.passed} passed"
    )

    if summary.failed > 0:
        sys.exit(1)
