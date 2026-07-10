import shutil
from pathlib import Path
from uuid import uuid4

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.types import IntegerType

from world_bank_pipeline.config import (
    OUTPUT_COLUMNS,
    REQUIRED_TOPIC_MAPPING_COLUMNS,
    SERIES_CODE_COLUMN,
    TOPIC_MAPPING_COLUMNS,
    TOPIC_OUTPUT_COLUMNS,
    VALUE_COLUMN,
    YEAR_COLUMN,
)
from world_bank_pipeline.transform import (
    convert_long_to_indicator_columns,
    convert_long_to_year_columns,
)
from world_bank_pipeline.validation import (
    get_trimmed_column_text,
    get_try_cast_expression,
    require_columns,
    validate_normalized_world_bank_long_data,
    validate_raw_world_bank_long_data,
)

PathInput = str | Path


def trim_text_column(column_name: str) -> Column:
    return sf.trim(sf.col(column_name).cast("string")).alias(column_name)


def select_world_bank_long_columns(dataframe: DataFrame) -> DataFrame:
    selected_columns: list[Column] = []
    year_text = get_trimmed_column_text(YEAR_COLUMN)
    value_text = get_trimmed_column_text(VALUE_COLUMN)
    value_as_double = get_try_cast_expression(VALUE_COLUMN, "double")

    for column_name in OUTPUT_COLUMNS:
        if column_name == YEAR_COLUMN:
            selected_columns.append(year_text.cast(IntegerType()).alias(column_name))
        elif column_name == VALUE_COLUMN:
            selected_columns.append(
                sf.when(value_text == "", sf.lit(None))
                .otherwise(value_as_double)
                .alias(column_name),
            )
        else:
            selected_columns.append(trim_text_column(column_name))

    return dataframe.select(*selected_columns)


def read_world_bank_long_parquet(spark: SparkSession, input_path: PathInput) -> DataFrame:
    dataframe = spark.read.parquet(str(input_path))
    raw_row_count = validate_raw_world_bank_long_data(dataframe)
    normalized_dataframe = select_world_bank_long_columns(dataframe)
    validate_normalized_world_bank_long_data(normalized_dataframe, raw_row_count)

    return normalized_dataframe


def read_indicator_topic_mapping(spark: SparkSession, mapping_path: PathInput) -> DataFrame:
    dataframe = (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(str(mapping_path))
    )
    require_columns(dataframe, REQUIRED_TOPIC_MAPPING_COLUMNS)

    return dataframe


def remove_existing_output(output_file: Path) -> None:
    if not output_file.exists():
        return

    if output_file.is_dir():
        shutil.rmtree(output_file)
    else:
        output_file.unlink()


def get_temporary_output_directory(output_directory: Path, suffix: str) -> Path:
    return output_directory.with_name(
        f".{output_directory.name}.{uuid4().hex}{suffix}",
    )


def write_parquet_dataset(dataframe: DataFrame, output_path: PathInput) -> None:
    output_directory = Path(output_path)
    output_directory.parent.mkdir(parents=True, exist_ok=True)
    temporary_output_directory = get_temporary_output_directory(output_directory, ".tmp")
    staging_directory = get_temporary_output_directory(output_directory, ".staging")

    try:
        dataframe.write.mode("overwrite").parquet(str(temporary_output_directory))

        temporary_output_directory.replace(staging_directory)
        remove_existing_output(output_directory)
        staging_directory.replace(output_directory)
    finally:
        if staging_directory.exists():
            shutil.rmtree(staging_directory)

        if temporary_output_directory.exists():
            shutil.rmtree(temporary_output_directory)


def get_single_csv_part_file(output_directory: Path) -> Path:
    part_files = sorted(output_directory.glob("part-*.csv"))

    if len(part_files) != 1:
        raise RuntimeError(
            f"Expected one CSV part file in {output_directory}, found {len(part_files)}.",
        )

    return part_files[0]


def write_single_csv(dataframe: DataFrame, output_path: PathInput) -> None:
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    temporary_output_directory = get_temporary_output_directory(output_file, ".tmp")
    staging_file = get_temporary_output_directory(output_file, ".staging")

    try:
        (
            dataframe.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(str(temporary_output_directory))
        )

        part_file = get_single_csv_part_file(temporary_output_directory)
        shutil.move(str(part_file), str(staging_file))
        remove_existing_output(output_file)
        staging_file.replace(output_file)
    finally:
        if staging_file.exists():
            staging_file.unlink()

        if temporary_output_directory.exists():
            shutil.rmtree(temporary_output_directory)


def write_inner_joined_indicator_topic_mapping_csv(
    topic_mapping: DataFrame,
    final_long_dataframe: DataFrame,
    output_path: PathInput,
) -> None:
    require_columns(topic_mapping, TOPIC_MAPPING_COLUMNS)
    final_indicator_codes = final_long_dataframe.select(
        sf.col(SERIES_CODE_COLUMN).alias("id"),
    ).distinct()
    mapping_dataframe = (
        topic_mapping.select(*TOPIC_MAPPING_COLUMNS)
        .join(sf.broadcast(final_indicator_codes), on="id", how="inner")
        .orderBy("id")
    )
    write_single_csv(mapping_dataframe, output_path)


def write_long_parquet_dataset(dataframe: DataFrame, output_path: PathInput) -> None:
    write_parquet_dataset(dataframe.select(*TOPIC_OUTPUT_COLUMNS), output_path)


def write_indicator_wide_parquet_dataset(dataframe: DataFrame, output_path: PathInput) -> None:
    indicator_column_dataframe = convert_long_to_indicator_columns(dataframe)
    write_parquet_dataset(indicator_column_dataframe, output_path)


def write_year_wide_parquet_dataset(dataframe: DataFrame, output_path: PathInput) -> None:
    year_column_dataframe = convert_long_to_year_columns(dataframe)
    write_parquet_dataset(year_column_dataframe, output_path)
