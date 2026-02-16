"""Tests for the ``fetch`` subcommand and ``fetch_table_to_file``."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from bq_sync.cli import _build_parser

# ---------------------------------------------------------------------------
# CLI parsing
# ---------------------------------------------------------------------------


class TestFetchCLIParsing:
    """Verify ``fetch`` subcommand argument parsing."""

    def test_defaults_to_csv(self) -> None:
        """``fetch`` without ``-f`` defaults to csv format."""
        parser = _build_parser()
        args = parser.parse_args(["fetch", "proj/ds/tbl"])

        assert args.command == "fetch"
        assert args.model == "proj/ds/tbl"
        assert args.format == "csv"
        assert args.output_dir is None

    def test_parquet_format(self) -> None:
        """``-f parquet`` is accepted."""
        parser = _build_parser()
        args = parser.parse_args(["fetch", "proj/ds/tbl", "-f", "parquet"])

        assert args.format == "parquet"

    def test_long_format_flag(self) -> None:
        """``--format parquet`` is accepted."""
        parser = _build_parser()
        args = parser.parse_args(["fetch", "proj/ds/tbl", "--format", "parquet"])

        assert args.format == "parquet"

    def test_output_dir(self) -> None:
        """``-o`` sets output directory."""
        parser = _build_parser()
        args = parser.parse_args(["fetch", "proj/ds/tbl", "-o", "/tmp/out"])

        assert args.output_dir == "/tmp/out"

    def test_invalid_format_rejected(self) -> None:
        """Unsupported format choice raises SystemExit."""
        parser = _build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["fetch", "proj/ds/tbl", "-f", "json"])

    def test_missing_model_rejected(self) -> None:
        """Missing positional model argument raises SystemExit."""
        parser = _build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["fetch"])

    def test_local_path_four_segments(self) -> None:
        """Local model path with resource-type directory is accepted."""
        parser = _build_parser()
        args = parser.parse_args(["fetch", "proj/ds/models/tbl"])

        assert args.model == "proj/ds/models/tbl"

    def test_local_path_with_extension(self) -> None:
        """Local model path with file extension is accepted."""
        parser = _build_parser()
        args = parser.parse_args(["fetch", "proj/ds/models/tbl.yaml"])

        assert args.model == "proj/ds/models/tbl.yaml"


# ---------------------------------------------------------------------------
# fetch_table_to_file
# ---------------------------------------------------------------------------


def _make_mock_schema(names: list[str]) -> list[MagicMock]:
    """Build a list of mock ``SchemaField`` objects."""
    fields = []
    for name in names:
        field = MagicMock()
        field.name = name
        fields.append(field)
    return fields


def _make_mock_rows(columns: list[str], data: list[list[object]]) -> list[MagicMock]:
    """Build mock row objects that support ``row[col]`` access."""
    rows = []
    for row_values in data:
        row = MagicMock()
        mapping = dict(zip(columns, row_values))
        row.__getitem__ = MagicMock(side_effect=lambda key, m=mapping: m[key])
        rows.append(row)
    return rows


class TestFetchTableToFile:
    """Tests for ``bq_client.fetch_table_to_file``."""

    @patch("bq_sync.bq_client.bigquery.Client")
    def test_csv_output(self, mock_client_cls: MagicMock, tmp_path: Path) -> None:
        """CSV file is created with correct header and data."""
        columns = ["id", "name"]
        data = [[1, "alice"], [2, "bob"]]

        mock_iter = MagicMock()
        mock_iter.schema = _make_mock_schema(columns)
        mock_iter.__iter__ = MagicMock(
            return_value=iter(_make_mock_rows(columns, data))
        )
        mock_client_cls.return_value.list_rows.return_value = mock_iter

        from bq_sync.bq_client import fetch_table_to_file

        dest = tmp_path / "data" / "tbl.csv"
        fetch_table_to_file("proj", "ds", "tbl", dest, fmt="csv")

        assert dest.exists()
        df = pl.read_csv(dest)
        assert df.columns == columns
        assert df.shape == (2, 2)
        assert df["name"].to_list() == ["alice", "bob"]

    @patch("bq_sync.bq_client.bigquery.Client")
    def test_parquet_output(self, mock_client_cls: MagicMock, tmp_path: Path) -> None:
        """Parquet file is created and readable."""
        columns = ["x", "y"]
        data = [[10, 20], [30, 40]]

        mock_iter = MagicMock()
        mock_iter.schema = _make_mock_schema(columns)
        mock_iter.__iter__ = MagicMock(
            return_value=iter(_make_mock_rows(columns, data))
        )
        mock_client_cls.return_value.list_rows.return_value = mock_iter

        from bq_sync.bq_client import fetch_table_to_file

        dest = tmp_path / "data" / "tbl.parquet"
        fetch_table_to_file("proj", "ds", "tbl", dest, fmt="parquet")

        assert dest.exists()
        df = pl.read_parquet(dest)
        assert df.columns == columns
        assert df.shape == (2, 2)

    def test_unsupported_format_raises(self, tmp_path: Path) -> None:
        """ValueError raised for unsupported format string."""
        from bq_sync.bq_client import fetch_table_to_file

        with pytest.raises(ValueError, match="Unsupported format"):
            fetch_table_to_file("proj", "ds", "tbl", tmp_path / "f.json", fmt="json")

    @patch("bq_sync.bq_client.bigquery.Client")
    def test_file_named_after_model(
        self, mock_client_cls: MagicMock, tmp_path: Path
    ) -> None:
        """Output file stem matches the model name."""
        columns = ["a"]
        mock_iter = MagicMock()
        mock_iter.schema = _make_mock_schema(columns)
        mock_iter.__iter__ = MagicMock(
            return_value=iter(_make_mock_rows(columns, [[1]]))
        )
        mock_client_cls.return_value.list_rows.return_value = mock_iter

        from bq_sync.bq_client import fetch_table_to_file

        dest = tmp_path / "my_table.csv"
        fetch_table_to_file("proj", "ds", "my_table", dest, fmt="csv")

        assert dest.stem == "my_table"
        assert dest.suffix == ".csv"
