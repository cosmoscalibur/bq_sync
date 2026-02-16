"""Tests for ``bq_sync.config``."""

from __future__ import annotations

from pathlib import Path

import pytest

from bq_sync.config import (
    CONFIG_FILENAME,
    discover_config,
    load_config,
    resolve_output_dir,
)

VALID_TOML = """\
[project]
id = "my-project"
default_region = "us-east1"

[sync]
datasets = ["ds1", "ds2"]
output_dir = "output"
"""

MINIMAL_TOML = """\
[project]
id = "proj"
default_region = "us"

[sync]
datasets = []
"""


class TestLoadConfig:
    """Tests for ``load_config``."""

    def test_valid_toml(self, tmp_path: Path) -> None:
        """Parse a well-formed config file."""
        cfg_path = tmp_path / CONFIG_FILENAME
        cfg_path.write_text(VALID_TOML)
        config = load_config(cfg_path)

        assert config.project.id == "my-project"
        assert config.project.default_region == "us-east1"
        assert config.datasets == ["ds1", "ds2"]
        assert config.output_dir == "output"

    def test_missing_required_field(self, tmp_path: Path) -> None:
        """Raise ``KeyError`` when a required key is missing."""
        cfg_path = tmp_path / CONFIG_FILENAME
        cfg_path.write_text("[project]\nid = 'x'\n[sync]\ndatasets = []\n")

        with pytest.raises(KeyError):
            load_config(cfg_path)

    def test_empty_datasets(self, tmp_path: Path) -> None:
        """Accept an empty datasets list."""
        cfg_path = tmp_path / CONFIG_FILENAME
        cfg_path.write_text(MINIMAL_TOML)
        config = load_config(cfg_path)

        assert config.datasets == []

    def test_default_output_dir(self, tmp_path: Path) -> None:
        """Default ``output_dir`` to ``.`` when omitted."""
        cfg_path = tmp_path / CONFIG_FILENAME
        cfg_path.write_text(MINIMAL_TOML)
        config = load_config(cfg_path)

        assert config.output_dir == "."


class TestDiscoverConfig:
    """Tests for ``discover_config``."""

    def test_finds_in_cwd(self, tmp_path: Path) -> None:
        """Discover config in the start directory."""
        cfg_path = tmp_path / CONFIG_FILENAME
        cfg_path.write_text(VALID_TOML)

        found = discover_config(start=tmp_path)
        assert found == cfg_path.resolve()

    def test_walks_up_to_parent(self, tmp_path: Path) -> None:
        """Discover config in a parent directory."""
        cfg_path = tmp_path / CONFIG_FILENAME
        cfg_path.write_text(VALID_TOML)
        child = tmp_path / "sub" / "deep"
        child.mkdir(parents=True)

        found = discover_config(start=child)
        assert found == cfg_path.resolve()

    def test_raises_when_absent(self, tmp_path: Path) -> None:
        """Raise ``FileNotFoundError`` when no config exists."""
        with pytest.raises(FileNotFoundError):
            discover_config(start=tmp_path)


class TestResolveOutputDir:
    """Tests for ``resolve_output_dir``."""

    def test_appends_project_id(self, tmp_path: Path) -> None:
        """Output dir is ``<base>/<output_dir>/<project_id>/``."""
        cfg_path = tmp_path / CONFIG_FILENAME
        cfg_path.write_text(VALID_TOML)
        config = load_config(cfg_path)

        result = resolve_output_dir(config, cfg_path)
        expected = (tmp_path / "output" / "my-project").resolve()
        assert result == expected

    def test_dot_output_dir(self, tmp_path: Path) -> None:
        """When ``output_dir`` is ``'.'``, output is ``<config_dir>/<project_id>``."""
        cfg_path = tmp_path / CONFIG_FILENAME
        cfg_path.write_text(MINIMAL_TOML)
        config = load_config(cfg_path)

        result = resolve_output_dir(config, cfg_path)
        expected = (tmp_path / "proj").resolve()
        assert result == expected
