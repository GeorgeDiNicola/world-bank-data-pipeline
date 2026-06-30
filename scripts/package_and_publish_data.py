import shutil
from pathlib import Path

import duckdb

OUTPUTS = {
    "world_bank_indicators_long.parquet/*.parquet":
        "world_bank_indicators_long.parquet",
    "world_bank_indicators_indicator_wide.parquet/*.parquet":
        "world_bank_indicators_indicator_wide.parquet",
    "world_bank_indicators_year_wide.parquet/*.parquet":
        "world_bank_indicators_year_wide.parquet",
}
INDICATOR_TOPIC_MAPPING_FILE = "indicator_topic_mapping.csv"


def combine_parquet_files(input_path: str | Path, output_path: str | Path) -> None:
    input_directory = Path(input_path)
    output_directory = Path(output_path)
    output_directory.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect()

    try:
        for source_glob, target_name in OUTPUTS.items():
            source_path = input_directory / source_glob
            target_file = output_directory / target_name
            csv_file = target_file.with_suffix(".csv")
            connection.execute(
                f"""
                COPY (
                    SELECT *
                    FROM read_parquet('{source_path}')
                )
                TO '{target_file}'
                (FORMAT PARQUET, COMPRESSION ZSTD);
                """,
            )
            connection.execute(
                f"""
                COPY (
                    SELECT *
                    FROM read_parquet('{source_path}')
                )
                TO '{csv_file}'
                (FORMAT CSV, HEADER);
                """,
            )
    finally:
        connection.close()


def upload_dataset_to_kaggle(upload_path: str | Path) -> None:
    import kaggle

    kaggle.api.authenticate()
    kaggle.api.dataset_create_version(
        folder=str(upload_path),
        version_notes="Updated dataset",
    )


def copy_indicator_topic_mapping(input_path: str | Path, output_path: str | Path) -> None:
    input_file = Path(input_path) / INDICATOR_TOPIC_MAPPING_FILE
    output_file = Path(output_path) / INDICATOR_TOPIC_MAPPING_FILE
    output_file.parent.mkdir(parents=True, exist_ok=True)

    shutil.copy2(input_file, output_file)


def main() -> None:
    combine_parquet_files(input_path="output", output_path="upload")
    copy_indicator_topic_mapping(input_path="output", output_path="upload")
    upload_dataset_to_kaggle(upload_path="upload")


if __name__ == "__main__":
    main()
