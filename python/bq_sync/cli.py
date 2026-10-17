"""Console entry point for the ``bq-sync`` command."""

from __future__ import annotations

import sys

from bq_sync._bq_sync import main as _main


def main(argv: list[str] | None = None) -> None:
    """Run the CLI with *argv* (default: ``sys.argv[1:]``) and exit."""
    sys.exit(_main(argv))


if __name__ == "__main__":
    main()
