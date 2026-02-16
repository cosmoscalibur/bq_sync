"""Fetch decision logic for pull operations.

Determines whether each BQ resource should be fetched, skipped, or
trigger a warning based on comparing BQ modification times against
git commit timestamps.
"""

from __future__ import annotations

import enum
import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


class FetchAction(enum.Enum):
    """Possible outcomes of the fetch decision matrix."""

    FETCH = "fetch"
    SKIP = "skip"
    WARN = "warn"


def git_committed_time(file: Path) -> datetime | None:
    """Return the last git commit time for *file*.

    Args:
        file: Path to a tracked file.

    Returns:
        Commit ``datetime`` in UTC, or ``None`` if the file has no
        git history.
    """
    try:
        result = subprocess.run(
            ["git", "log", "-1", "--format=%cI", "--", str(file)],
            capture_output=True,
            text=True,
            check=True,
            cwd=file.parent,
        )
        raw = result.stdout.strip()
        if not raw:
            return None
        return datetime.fromisoformat(raw).astimezone(timezone.utc)
    except (subprocess.CalledProcessError, OSError):
        return None


def has_uncommitted_changes(output_root: Path) -> bool:
    """Check whether *output_root* contains uncommitted tracked changes.

    Uses ``git status --porcelain`` scoped to *output_root*.

    Args:
        output_root: Directory to check.

    Returns:
        ``True`` if there are uncommitted changes in tracked files.
    """
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain", "--", str(output_root)],
            capture_output=True,
            text=True,
            check=True,
            cwd=output_root if output_root.is_dir() else output_root.parent,
        )
        return bool(result.stdout.strip())
    except (subprocess.CalledProcessError, OSError):
        return False


def resolve(
    bq_modified: datetime | None,
    file: Path,
    *,
    force: bool = False,
) -> FetchAction:
    """Apply the fetch decision matrix.

    Args:
        bq_modified: BQ resource modification time, or ``None`` when the
            resource does not exist on BigQuery.
        file: Local file path for the resource.
        force: When ``True``, bypass the decision matrix and always
            return ``FETCH``.

    Returns:
        The ``FetchAction`` that should be taken.

    Decision matrix (see ``implementation_plan.md``):

    ============  ============  ============  =========================  ======
    BQ exists?    File exists?  Git history?  Condition                  Action
    ============  ============  ============  =========================  ======
    Yes           No            —             —                          FETCH
    Yes           Yes           No            —                          WARN
    Yes           Yes           Yes           BQ mod ≤ git committed     SKIP
    Yes           Yes           Yes           BQ mod > git committed     FETCH
    No            No            —             —                          SKIP
    No            Yes           Yes           —                          WARN
    ============  ============  ============  =========================  ======
    """
    if force:
        return FetchAction.FETCH

    bq_exists = bq_modified is not None
    file_exists = file.is_file()

    if not bq_exists and not file_exists:
        return FetchAction.SKIP

    if not bq_exists and file_exists:
        # Resource deleted on BQ, local committed copy exists.
        logger.warning(
            "Resource deleted on BQ but local file exists: %s",
            file,
        )
        return FetchAction.WARN

    # bq_exists is True from here.
    if not file_exists:
        return FetchAction.FETCH

    # Both exist — check git history.
    committed = git_committed_time(file)
    if committed is None:
        # File exists but not committed — pending commit.
        logger.warning(
            "Local file not committed, cannot compare: %s",
            file,
        )
        return FetchAction.WARN

    assert bq_modified is not None  # Narrowing for type checker.
    if bq_modified <= committed:
        return FetchAction.SKIP

    # BQ is more recent than last commit — fetch and warn.
    logger.warning(
        "BQ resource is more recent than committed version: %s (BQ: %s, committed: %s)",
        file,
        bq_modified.isoformat(),
        committed.isoformat(),
    )
    return FetchAction.FETCH
