from pathlib import Path
from typing import cast

import duckdb
import pytest

import scripts.package_and_publish_data as package_and_publish_data


def quote_path(path: Path) -> str:
    return str(path).replace("'", "''")


def write_part_file(part_file: Path, rows: list[tuple[str, int]]) -> None:
    """Write a Parquet part file with the given rows."""
    part_file.parent.mkdir(parents=True, exist_ok=True)
    values_sql = ", ".join(
        f"('{name}', {value})"
        for name, value in rows
    )
    connection = duckdb.connect()
    connection.execute(
        f"""
        COPY (
            SELECT *
            FROM (VALUES {values_sql}) AS rows(name, value)
        )
        TO '{quote_path(part_file)}'
        (FORMAT PARQUET);
        """,
    )
    connection.close()


def read_rows(parquet_file: Path) -> list[tuple[str, int]]:
    connection = duckdb.connect()
    rows = connection.execute(
        f"""
        SELECT name, value
        FROM read_parquet('{quote_path(parquet_file)}')
        ORDER BY name
        """,
    ).fetchall()
    connection.close()

    return cast(list[tuple[str, int]], rows)


def read_csv_rows(csv_file: Path) -> list[tuple[str, int]]:
    connection = duckdb.connect()
    rows = connection.execute(
        f"""
        SELECT name, value
        FROM read_csv('{quote_path(csv_file)}', header = true)
        ORDER BY name
        """,
    ).fetchall()
    connection.close()

    return cast(list[tuple[str, int]], rows)


def test_combine_parquet_files_combines_the_standard_pipeline_outputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    input_directory = tmp_path / "input"
    output_directory = tmp_path / "published"

    for source_glob in package_and_publish_data.OUTPUTS:
        source_directory = input_directory / source_glob.removesuffix("/*.parquet")
        write_part_file(source_directory / "part-00000.parquet", [("Argentina", 1)])
        write_part_file(source_directory / "part-00001.parquet", [("Zimbabwe", 2)])

    package_and_publish_data.combine_parquet_files(
        input_path=input_directory,
        output_path=output_directory,
    )

    for target_file in package_and_publish_data.OUTPUTS.values():
        parquet_file = output_directory / target_file
        csv_file = parquet_file.with_suffix(".csv")

        assert read_rows(parquet_file) == [
            ("Argentina", 1),
            ("Zimbabwe", 2),
        ]
        assert read_csv_rows(csv_file) == [
            ("Argentina", 1),
            ("Zimbabwe", 2),
        ]
