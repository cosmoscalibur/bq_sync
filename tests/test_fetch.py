"""Tests for ``bq_sync.fetch``."""

from __future__ import annotations

import subprocess
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from bq_sync.fetch import (
    FetchAction,
    git_committed_time,
    has_uncommitted_changes,
    resolve,
)

# Reusable timestamps
BQ_OLD = datetime(2024, 1, 1, tzinfo=timezone.utc)
BQ_NEW = datetime(2025, 6, 1, tzinfo=timezone.utc)
GIT_MID = datetime(2025, 1, 1, tzinfo=timezone.utc)


class TestGitCommittedTime:
    """Tests for ``git_committed_time``."""

    def test_returns_none_for_untracked_file(self, tmp_path: Path) -> None:
        """Untracked files have no git history."""
        # Initialize a git repo so the command can run.
        subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True)
        f = tmp_path / "untracked.sql"
        f.write_text("SELECT 1")

        assert git_committed_time(f) is None

    def test_returns_datetime_for_committed_file(self, tmp_path: Path) -> None:
        """Committed files return a UTC datetime."""
        subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True)
        subprocess.run(
            ["git", "config", "user.email", "test@test.com"],
            cwd=tmp_path,
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["git", "config", "user.name", "Test"],
            cwd=tmp_path,
            check=True,
            capture_output=True,
        )
        f = tmp_path / "tracked.sql"
        f.write_text("SELECT 1")
        subprocess.run(
            ["git", "add", "."], cwd=tmp_path, check=True, capture_output=True
        )
        subprocess.run(
            ["git", "commit", "-m", "init"],
            cwd=tmp_path,
            check=True,
            capture_output=True,
        )

        result = git_committed_time(f)
        assert result is not None
        assert result.tzinfo is not None


class TestHasUncommittedChanges:
    """Tests for ``has_uncommitted_changes``."""

    def test_dirty_tree(self, tmp_path: Path) -> None:
        """Detect uncommitted tracked changes."""
        subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True)
        subprocess.run(
            ["git", "config", "user.email", "test@test.com"],
            cwd=tmp_path,
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["git", "config", "user.name", "Test"],
            cwd=tmp_path,
            check=True,
            capture_output=True,
        )
        f = tmp_path / "file.sql"
        f.write_text("v1")
        subprocess.run(
            ["git", "add", "."], cwd=tmp_path, check=True, capture_output=True
        )
        subprocess.run(
            ["git", "commit", "-m", "init"],
            cwd=tmp_path,
            check=True,
            capture_output=True,
        )
        # Modify tracked file.
        f.write_text("v2")

        assert has_uncommitted_changes(tmp_path) is True

    def test_clean_tree(self, tmp_path: Path) -> None:
        """Clean tree reports no uncommitted changes."""
        subprocess.run(["git", "init"], cwd=tmp_path, check=True, capture_output=True)
        subprocess.run(
            ["git", "config", "user.email", "test@test.com"],
            cwd=tmp_path,
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["git", "config", "user.name", "Test"],
            cwd=tmp_path,
            check=True,
            capture_output=True,
        )
        f = tmp_path / "file.sql"
        f.write_text("v1")
        subprocess.run(
            ["git", "add", "."], cwd=tmp_path, check=True, capture_output=True
        )
        subprocess.run(
            ["git", "commit", "-m", "init"],
            cwd=tmp_path,
            check=True,
            capture_output=True,
        )

        assert has_uncommitted_changes(tmp_path) is False


class TestResolve:
    """Tests for ``resolve`` — the fetch decision matrix."""

    def test_bq_yes_file_no(self, tmp_path: Path) -> None:
        """BQ exists, file does not → FETCH."""
        path = tmp_path / "view.sql"
        assert resolve(BQ_NEW, path) == FetchAction.FETCH

    def test_bq_yes_file_yes_no_git(self, tmp_path: Path) -> None:
        """BQ exists, file exists, no git history → WARN."""
        path = tmp_path / "view.sql"
        path.write_text("SELECT 1")

        with patch("bq_sync.fetch.git_committed_time", return_value=None):
            assert resolve(BQ_NEW, path) == FetchAction.WARN

    def test_bq_yes_file_yes_bq_older(self, tmp_path: Path) -> None:
        """BQ modified ≤ git committed → SKIP."""
        path = tmp_path / "view.sql"
        path.write_text("SELECT 1")

        with patch("bq_sync.fetch.git_committed_time", return_value=GIT_MID):
            assert resolve(BQ_OLD, path) == FetchAction.SKIP

    def test_bq_yes_file_yes_bq_newer(self, tmp_path: Path) -> None:
        """BQ modified > git committed → FETCH."""
        path = tmp_path / "view.sql"
        path.write_text("SELECT 1")

        with patch("bq_sync.fetch.git_committed_time", return_value=GIT_MID):
            assert resolve(BQ_NEW, path) == FetchAction.FETCH

    def test_bq_no_file_no(self, tmp_path: Path) -> None:
        """Neither BQ nor file exist → SKIP."""
        path = tmp_path / "view.sql"
        assert resolve(None, path) == FetchAction.SKIP

    def test_bq_no_file_yes(self, tmp_path: Path) -> None:
        """BQ deleted but file exists → WARN."""
        path = tmp_path / "view.sql"
        path.write_text("SELECT 1")

        assert resolve(None, path) == FetchAction.WARN

    def test_force_mode(self, tmp_path: Path) -> None:
        """Force mode always returns FETCH."""
        path = tmp_path / "view.sql"
        path.write_text("SELECT 1")

        with patch("bq_sync.fetch.git_committed_time", return_value=GIT_MID):
            assert resolve(BQ_OLD, path, force=True) == FetchAction.FETCH

    def test_force_on_nonexistent_file(self, tmp_path: Path) -> None:
        """Force mode on nonexistent file → FETCH."""
        path = tmp_path / "view.sql"
        assert resolve(BQ_NEW, path, force=True) == FetchAction.FETCH
