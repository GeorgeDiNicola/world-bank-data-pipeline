from pathlib import Path

import pytest

from src.world_bank_pipeline.io import (
    get_single_part_file,
    remove_existing_output,
)


def test_remove_existing_output_allows_missing_file(tmp_path: Path) -> None:
    output_file = tmp_path / "world_bank_indicators_long.csv"

    remove_existing_output(output_file)

    assert not output_file.exists()


def test_remove_existing_output_removes_existing_file(tmp_path: Path) -> None:
    """An existing single CSV file is removed before writing."""
    output_file = tmp_path / "world_bank_indicators_long.csv"
    output_file.write_text("existing\n")

    remove_existing_output(output_file)

    assert not output_file.exists()


def test_remove_existing_output_removes_existing_directory(tmp_path: Path) -> None:
    """A stale directory at the single CSV path is removed before writing."""
    output_directory = tmp_path / "world_bank_indicators_long.csv"
    output_directory.mkdir()
    (output_directory / "part-00000.csv").write_text("existing\n")

    remove_existing_output(output_directory)

    assert not output_directory.exists()


def test_get_single_part_file_returns_only_part_file(tmp_path: Path) -> None:
    """Exactly one Spark part file can be selected from an output directory."""
    part_file = tmp_path / "part-00000-test.csv"
    part_file.write_text("header\nvalue\n")
    (tmp_path / "_SUCCESS").write_text("")

    assert get_single_part_file(tmp_path) == part_file


def test_get_single_part_file_requires_exactly_one_part_file(tmp_path: Path) -> None:
    """Zero or multiple Spark part files are treated as invalid."""
    with pytest.raises(RuntimeError):
        get_single_part_file(tmp_path)

    (tmp_path / "part-00000-test.csv").write_text("header\n")
    (tmp_path / "part-00001-test.csv").write_text("header\n")

    with pytest.raises(RuntimeError):
        get_single_part_file(tmp_path)
