from pathlib import Path

import pytest

from world_bank_pipeline.io import (
    get_single_part_file,
    get_topic_indicator_column_output_path,
    get_topic_output_paths,
    get_topic_output_path,
    get_topic_wide_output_path,
    remove_existing_output,
    slugify_topic_name,
    write_single_csv,
)


class SuccessfulCsvWriter:
    def __init__(self, output_text: str) -> None:
        self.output_text = output_text

    def mode(self, mode: str) -> "SuccessfulCsvWriter":
        return self

    def option(self, key: str, value: object) -> "SuccessfulCsvWriter":
        return self

    def csv(self, output_path: str) -> None:
        output_directory = Path(output_path)
        output_directory.mkdir(parents=True)
        (output_directory / "part-00000-test.csv").write_text(self.output_text)
        (output_directory / "_SUCCESS").write_text("")


class FailingCsvWriter:
    def mode(self, mode: str) -> "FailingCsvWriter":
        return self

    def option(self, key: str, value: object) -> "FailingCsvWriter":
        return self

    def csv(self, output_path: str) -> None:
        raise RuntimeError("write failed")


class MockSparkDataFrame:
    def __init__(self, writer: SuccessfulCsvWriter | FailingCsvWriter) -> None:
        self._writer = writer

    def coalesce(self, partitions: int) -> "MockSparkDataFrame":
        return self

    @property
    def write(self) -> SuccessfulCsvWriter | FailingCsvWriter:
        return self._writer


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


def test_write_single_csv_replaces_existing_output_after_new_file_is_ready(
    tmp_path: Path,
) -> None:
    output_file = tmp_path / "world_bank_indicators_long.csv"
    output_file.write_text("old\n")
    dataframe = MockSparkDataFrame(SuccessfulCsvWriter("new\n"))

    write_single_csv(dataframe, output_file)

    assert output_file.read_text() == "new\n"
    assert not list(tmp_path.glob("*.tmp"))
    assert not list(tmp_path.glob("*.staging"))


def test_write_single_csv_preserves_existing_output_when_write_fails(tmp_path: Path) -> None:
    output_file = tmp_path / "world_bank_indicators_long.csv"
    output_file.write_text("old\n")
    dataframe = MockSparkDataFrame(FailingCsvWriter())

    with pytest.raises(RuntimeError, match="write failed"):
        write_single_csv(dataframe, output_file)

    assert output_file.read_text() == "old\n"

    (tmp_path / "part-00000-test.csv").write_text("header\n")
    (tmp_path / "part-00001-test.csv").write_text("header\n")

    with pytest.raises(RuntimeError):
        get_single_part_file(tmp_path)


def test_get_topic_output_path_uses_topic_name_as_csv_file(tmp_path: Path) -> None:
    output_path = get_topic_output_path(tmp_path, "Agriculture & Rural Development")

    assert output_path == tmp_path / "agriculture_rural_development_long.csv"


def test_get_topic_indicator_column_output_path_omits_long_suffix(tmp_path: Path) -> None:
    output_path = get_topic_indicator_column_output_path(
        tmp_path,
        "Agriculture & Rural Development",
    )

    assert output_path == tmp_path / "agriculture_rural_development.csv"


def test_get_topic_wide_output_path_uses_wide_suffix(tmp_path: Path) -> None:
    output_path = get_topic_wide_output_path(
        tmp_path,
        "Agriculture & Rural Development",
    )

    assert output_path == tmp_path / "agriculture_rural_development_wide.csv"


def test_slugify_topic_name_uses_lowercase_letters_numbers_and_underscores() -> None:
    assert slugify_topic_name("Science & Technology 2024!") == "science_technology_2024"


def test_slugify_topic_name_rejects_names_without_letters_or_numbers() -> None:
    with pytest.raises(ValueError, match="at least one letter or number"):
        slugify_topic_name("!!!")


def test_get_topic_output_paths_rejects_slug_collisions(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="produce the same output file"):
        get_topic_output_paths(tmp_path, ["A&B", "A B"], get_topic_output_path)
