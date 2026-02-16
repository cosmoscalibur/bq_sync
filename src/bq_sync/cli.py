"""CLI entrypoint for bq-sync."""

from __future__ import annotations

import argparse
import logging
import sys

from bq_sync.config import discover_config, load_config
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


def _handle_pull(args: argparse.Namespace) -> None:
    """Handle the ``pull`` subcommand."""
    from pathlib import Path

    # Discover or use explicit config path.
    if args.config:
        config_path = Path(args.config).resolve()
    else:
        try:
            config_path = discover_config()
        except FileNotFoundError as exc:
            logging.error("%s", exc)
            sys.exit(1)

    config = load_config(config_path)

    # If --dataset narrows the scope, override config.
    if args.dataset:
        # Replace datasets list with the single requested dataset.
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
