import glob
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def get_input_paths(input_path: str) -> list[str]:
    input_paths = sorted(glob.glob(input_path))

    if input_paths:
        return input_paths

    raise FileNotFoundError(f"No input files found for path: {input_path}")


def read_world_bank_csv(spark: SparkSession, input_path: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(input_path)
    )


def read_multiple_world_bank_csvs(spark: SparkSession, input_path: str) -> list[DataFrame]:
    return [read_world_bank_csv(spark, path) for path in get_input_paths(input_path)]


def write_long_csv(dataframe: DataFrame, output_path: str) -> None:
    (
        dataframe.write.mode("overwrite")
        .option("header", True)
        .csv(output_path)
    )


def remove_existing_output(output_file: Path) -> None:
    if not output_file.exists():
        return

    if output_file.is_dir():
        shutil.rmtree(output_file)
    else:
        output_file.unlink()


def get_single_part_file(output_directory: Path) -> Path:
    part_files = sorted(output_directory.glob("part-*.csv"))

    if len(part_files) != 1:
        raise RuntimeError(
            f"Expected one CSV part file in {output_directory}, found {len(part_files)}.",
        )

    return part_files[0]


def write_single_csv(dataframe: DataFrame, output_path: str) -> None:
    output_file = Path(output_path)
    remove_existing_output(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    temporary_output_directory = output_file.with_name(f"{output_file.name}.tmp")

    if temporary_output_directory.exists():
        shutil.rmtree(temporary_output_directory)

    try:
        (
            dataframe.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(str(temporary_output_directory))
        )

        part_file = get_single_part_file(temporary_output_directory)
        shutil.move(str(part_file), output_file)
    finally:
        if temporary_output_directory.exists():
            shutil.rmtree(temporary_output_directory)
