from pathlib import Path

import duckdb

OUTPUTS = {
    "output/world_bank_indicators_long.parquet/*.parquet":
        "output/world_bank_indicators_long_single.parquet",
    "output/world_bank_indicators_indicator_wide.parquet/*.parquet":
        "output/world_bank_indicators_indicator_wide_single.parquet",
    "output/world_bank_indicators_year_wide.parquet/*.parquet":
        "output/world_bank_indicators_year_wide_single.parquet",
}


def main() -> None:
    Path("output").mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect()

    for source_glob, target_file in OUTPUTS.items():
        connection.execute(
            f"""
            COPY (
                SELECT *
                FROM read_parquet('{source_glob}')
            )
            TO '{target_file}'
            (FORMAT PARQUET, COMPRESSION ZSTD);
            """,
        )

    connection.close()


if __name__ == "__main__":
    main()
