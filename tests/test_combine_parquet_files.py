from pathlib import Path
from typing import cast

import duckdb
import pytest

import combine_parquet_files


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


def test_main_combines_the_standard_pipeline_outputs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)

    for source_glob in combine_parquet_files.OUTPUTS:
        source_directory = tmp_path / source_glob.removesuffix("/*.parquet")
        write_part_file(source_directory / "part-00000.parquet", [("Argentina", 1)])
        write_part_file(source_directory / "part-00001.parquet", [("Zimbabwe", 2)])

    combine_parquet_files.main()

    for target_file in combine_parquet_files.OUTPUTS.values():
        assert read_rows(tmp_path / target_file) == [
            ("Argentina", 1),
            ("Zimbabwe", 2),
        ]
