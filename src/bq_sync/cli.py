"""CLI entrypoint for bq-sync."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from bq_sync.config import SyncConfig, discover_config, load_config
from bq_sync.pull import pull_project


def _build_parser() -> argparse.ArgumentParser:
    """Build the argument parser with ``pull`` and ``push`` subcommands."""
    parser = argparse.ArgumentParser(
        prog="bq-sync",
        description="Sync BigQuery resources to a local directory.",
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

    # --- push (placeholder) ---
    subparsers.add_parser(
        "push",
        help="Deploy local resources to BigQuery (not yet implemented).",
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
        help="Directory where a 'data/' folder is created (default: config data dir).",
    )
    fetch_parser.add_argument(
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
    logging.basicConfig(
        level=level,
        format="%(levelname)s: %(message)s",
    )

    if args.command == "push":
        from bq_sync.push import push_project

        push_project()

    if args.command == "pull":
        _handle_pull(args)

    if args.command == "fetch":
        _handle_fetch(args)


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


def _handle_fetch(args: argparse.Namespace) -> None:
    """Handle the ``fetch`` subcommand."""
    from bq_sync import bq_client
    from bq_sync.config import resolve_output_dir

    parts = args.model.split("/")
    if len(parts) == 4:
        # Local path: <project>/<dataset>/<resource_type>/<name[.ext]>
        project, dataset, _, name = parts
    elif len(parts) == 3:
        # BQ path: <project>/<dataset>/<name[.ext]>
        project, dataset, name = parts
    else:
        logging.error(
            "Invalid model path '%s': expected "
            "<project>/<dataset>/<table_or_view> or "
            "<project>/<dataset>/<resource_type>/<table_or_view> "
            "but got %d segments.",
            args.model,
            len(parts),
        )
        sys.exit(1)

    # Strip file extension if present (e.g. ".yaml", ".sql").
    model = Path(name).stem
    fmt: str = args.format

    # Resolve output directory.
    if args.output_dir:
        data_dir = Path(args.output_dir).resolve() / "data"
    else:
        config_path, config = _resolve_config(args)
        data_dir = resolve_output_dir(config, config_path) / "data"

    dest = data_dir / f"{model}.{fmt}"

    logging.info("Fetching %s -> %s", args.model, dest)
    bq_client.fetch_table_to_file(project, dataset, model, dest, fmt=fmt)
    logging.info("Saved %s", dest)
