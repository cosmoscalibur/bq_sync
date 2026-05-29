"""Shared utility helpers used across multiple bq-sync modules."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

# Resource directory names recognised in the local output tree.
# Kept here so every module (push, materialize, rm …) uses the same set.
_RESOURCE_DIRS = {"models", "views", "routines", "saved_queries"}


def parse_resource_path(resource_path: str) -> tuple[str, str, str]:
    """Parse a CLI resource path string into ``(project, dataset, name)``.

    Shared across ``push``, ``rm``, ``pull``, and ``materialize`` so that all
    commands accept the same two path forms:

    - **BQ tree** (3 parts): ``<project>/<dataset>/<name>[.yaml|.sql]``
      — the ``<name>`` extension is stripped when present.
    - **Local tree** (4 parts):
      ``<project>/<dataset>/<resource_dir>/<name>[.yaml|.sql]``
      — mirrors the directory layout produced by ``bq-sync pull``;
      the ``<resource_dir>`` segment (e.g. ``models``, ``views``) is dropped.

    Args:
        resource_path: Path string supplied on the CLI.

    Returns:
        Tuple of ``(project, dataset, name)`` with no file extension.

    Raises:
        SystemExit: When the path has an unrecognised number of segments.
    """
    parts = resource_path.split("/")

    if len(parts) == 3:
        project, dataset, name = parts
        return project, dataset, Path(name).stem  # strip .yaml / .sql

    if len(parts) == 4:
        project, dataset, resource_dir, name = parts
        if resource_dir not in _RESOURCE_DIRS:
            logger.warning(
                "Unknown resource directory '%s' in path '%s'; "
                "treating as a BQ path segment.",
                resource_dir,
                resource_path,
            )
        return project, dataset, Path(name).stem

    logger.error(
        "Invalid resource path '%s': expected "
        "<project>/<dataset>/<name> or "
        "<project>/<dataset>/<resource_dir>/<name>.",
        resource_path,
    )
    sys.exit(1)
