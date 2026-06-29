import os
from dataclasses import dataclass
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession

from world_bank_pipeline.config import (
    API_INDICATOR_OUTPUT_PATH,
    DEFAULT_SPARK_SQL_SHUFFLE_PARTITIONS,
    INDICATOR_WIDE_OUTPUT_PATH,
    LONG_OUTPUT_PATH,
    MAPPING_OUTPUT_PATH,
    MAPPING_PATH,
    SPARK_APP_NAME,
    SPARK_SQL_SHUFFLE_PARTITIONS_ENV_VAR,
    YEAR_WIDE_OUTPUT_PATH,
)
from world_bank_pipeline.io import (
    read_indicator_topic_mapping,
    read_world_bank_long_parquet,
    write_indicator_wide_parquet_dataset,
    write_inner_joined_indicator_topic_mapping_csv,
    write_long_parquet_dataset,
    write_year_wide_parquet_dataset,
)
from world_bank_pipeline.transform import (
    add_topics_to_long_data,
    keep_only_countries_and_territories,
    keep_only_rows_with_values,
)


@dataclass(frozen=True)
class PipelinePaths:
    api_indicator_output_path: Path = API_INDICATOR_OUTPUT_PATH
    mapping_path: Path = MAPPING_PATH
    long_output_path: Path = LONG_OUTPUT_PATH
    indicator_wide_output_path: Path = INDICATOR_WIDE_OUTPUT_PATH
    year_wide_output_path: Path = YEAR_WIDE_OUTPUT_PATH
    mapping_output_path: Path = MAPPING_OUTPUT_PATH


DEFAULT_PIPELINE_PATHS = PipelinePaths()


def get_spark_sql_shuffle_partitions() -> int:
    raw_partition_count = os.getenv(
        SPARK_SQL_SHUFFLE_PARTITIONS_ENV_VAR,
        str(DEFAULT_SPARK_SQL_SHUFFLE_PARTITIONS),
    )

    try:
        partition_count = int(raw_partition_count)
    except ValueError as error:
        raise ValueError(
            f"{SPARK_SQL_SHUFFLE_PARTITIONS_ENV_VAR} must be a positive integer.",
        ) from error

    if partition_count < 1:
        raise ValueError(
            f"{SPARK_SQL_SHUFFLE_PARTITIONS_ENV_VAR} must be a positive integer.",
        )

    return partition_count


def create_spark_session() -> SparkSession:
    shuffle_partitions = get_spark_sql_shuffle_partitions()

    return (
        SparkSession.builder.appName(SPARK_APP_NAME)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .getOrCreate()
    )


def run_pipeline(paths: PipelinePaths = DEFAULT_PIPELINE_PATHS) -> None:
    spark = create_spark_session()
    topic_dataframe: DataFrame | None = None

    try:
        long_dataframe = read_world_bank_long_parquet(spark, paths.api_indicator_output_path)
        topic_mapping = read_indicator_topic_mapping(spark, paths.mapping_path)
        long_dataframe = keep_only_countries_and_territories(
            keep_only_rows_with_values(long_dataframe),
        )
        topic_dataframe = (
            add_topics_to_long_data(long_dataframe, topic_mapping)
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        # The joined data feeds three write actions, populate the cache once
        topic_dataframe.count()
        write_inner_joined_indicator_topic_mapping_csv(
            topic_mapping,
            topic_dataframe,
            paths.mapping_output_path,
        )
        write_long_parquet_dataset(topic_dataframe, paths.long_output_path)
        write_indicator_wide_parquet_dataset(topic_dataframe, paths.indicator_wide_output_path)
        write_year_wide_parquet_dataset(topic_dataframe, paths.year_wide_output_path)
    finally:
        if topic_dataframe is not None:
            topic_dataframe.unpersist()

        spark.stop()


def main(paths: PipelinePaths = DEFAULT_PIPELINE_PATHS) -> None:
    run_pipeline(paths)
