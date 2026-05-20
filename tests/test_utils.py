"""Tests for ``bq_sync.utils``."""

from __future__ import annotations

from bq_sync.utils import parse_resource_path

# ---------------------------------------------------------------------------
# parse_resource_path
# ---------------------------------------------------------------------------


class TestParseResourcePath:
    """Tests for the two accepted CLI path forms."""

    def test_three_part_bq_path(self) -> None:
        """Plain ``project/dataset/name`` returns the three parts."""
        assert parse_resource_path("proj/ds/view_foo") == (
            "proj",
            "ds",
            "view_foo",
        )

    def test_three_part_with_yaml_extension(self) -> None:
        """Extension is stripped from a 3-part path."""
        assert parse_resource_path(
            "proj/ds/external_nps_contadia.yaml"
        ) == ("proj", "ds", "external_nps_contadia")

    def test_three_part_with_sql_extension(self) -> None:
        """`.sql` extension is stripped from a 3-part path."""
        assert parse_resource_path(
            "proj/ds/view_foo.sql"
        ) == ("proj", "ds", "view_foo")

    def test_four_part_local_models_path(self) -> None:
        """4-part local-tree path strips the resource_dir segment."""
        assert parse_resource_path(
            "proj/ds/models/external_nps_contadia.yaml"
        ) == ("proj", "ds", "external_nps_contadia")

    def test_four_part_local_views_path(self) -> None:
        """4-part views/ path strips the resource_dir segment."""
        assert parse_resource_path(
            "proj/ds/views/view_auth_user.sql"
        ) == ("proj", "ds", "view_auth_user")
