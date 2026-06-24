from pathlib import Path

import pytest

from world_bank_pipeline.io import (
    remove_existing_output,
    write_parquet_dataset,
)


class SuccessfulParquetWriter:
    def __init__(self, output_text: str) -> None:
        self.mode_value: str | None = None
        self.output_text = output_text

    def mode(self, mode: str) -> "SuccessfulParquetWriter":
        self.mode_value = mode
        return self

    def parquet(self, output_path: str) -> None:
        output_directory = Path(output_path)
        output_directory.mkdir(parents=True)
        (output_directory / "part-00000-test.snappy.parquet").write_text(self.output_text)
        (output_directory / "_SUCCESS").write_text("")


class FailingParquetWriter:
    def mode(self, mode: str) -> "FailingParquetWriter":
        return self

    def parquet(self, output_path: str) -> None:
        raise RuntimeError("write failed")


class MockSparkDataFrame:
    def __init__(self, writer: SuccessfulParquetWriter | FailingParquetWriter) -> None:
        self._writer = writer

    @property
    def write(self) -> SuccessfulParquetWriter | FailingParquetWriter:
        return self._writer


def test_remove_existing_output_allows_missing_file(tmp_path: Path) -> None:
    output_file = tmp_path / "world_bank_indicators_long.parquet"

    remove_existing_output(output_file)

    assert not output_file.exists()


def test_remove_existing_output_removes_existing_file(tmp_path: Path) -> None:
    """An existing file is removed before writing."""
    output_file = tmp_path / "world_bank_indicators_long.parquet"
    output_file.write_text("existing\n")

    remove_existing_output(output_file)

    assert not output_file.exists()


def test_remove_existing_output_removes_existing_directory(tmp_path: Path) -> None:
    """A stale directory at the output path is removed before writing."""
    output_directory = tmp_path / "world_bank_indicators_long.parquet"
    output_directory.mkdir()
    (output_directory / "part-00000-test.snappy.parquet").write_text("existing\n")

    remove_existing_output(output_directory)

    assert not output_directory.exists()


def test_write_parquet_dataset_replaces_existing_output_after_new_dataset_is_ready(
    tmp_path: Path,
) -> None:
    output_directory = tmp_path / "world_bank_indicators_long.parquet"
    output_directory.mkdir()
    (output_directory / "part-00000-old.snappy.parquet").write_text("old\n")
    writer = SuccessfulParquetWriter("new\n")
    dataframe = MockSparkDataFrame(writer)

    write_parquet_dataset(dataframe, output_directory)

    assert writer.mode_value == "overwrite"
    assert (output_directory / "part-00000-test.snappy.parquet").read_text() == "new\n"
    assert not (output_directory / "part-00000-old.snappy.parquet").exists()
    assert not list(tmp_path.glob(".*.tmp"))
    assert not list(tmp_path.glob(".*.staging"))


def test_write_parquet_dataset_preserves_existing_output_when_write_fails(
    tmp_path: Path,
) -> None:
    output_directory = tmp_path / "world_bank_indicators_long.parquet"
    output_directory.mkdir()
    (output_directory / "part-00000-old.snappy.parquet").write_text("old\n")
    dataframe = MockSparkDataFrame(FailingParquetWriter())

    with pytest.raises(RuntimeError, match="write failed"):
        write_parquet_dataset(dataframe, output_directory)

    assert (output_directory / "part-00000-old.snappy.parquet").read_text() == "old\n"
