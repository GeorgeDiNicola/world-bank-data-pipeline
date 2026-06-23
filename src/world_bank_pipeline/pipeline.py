from dataclasses import dataclass
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession

from world_bank_pipeline.io import (
    read_indicator_topic_mapping,
    read_world_bank_long_csv,
    write_long_csv,
    write_single_csv,
    write_topic_indicator_column_csvs,
    write_topic_csvs,
    write_topic_wide_csvs,
)
from world_bank_pipeline.transform import (
    add_topics_to_long_data,
    keep_only_countries_and_territories,
    keep_only_rows_with_values,
)

API_INDICATOR_OUTPUT_PATH = Path("/data/world_bank_api_indicator_data.csv")
MAPPING_PATH = "/mapping/indicator_topic_mapping.csv"
PARTITIONED_OUTPUT_PATH = "/output/world_bank_indicators_long"
SINGLE_OUTPUT_PATH = "/output/world_bank_indicators_long.csv"
TOPIC_OUTPUT_PATH = "/output/world_bank_indicators_by_topic"


@dataclass(frozen=True)
class PipelinePaths:
    api_indicator_output_path: Path = API_INDICATOR_OUTPUT_PATH
    mapping_path: Path = Path(MAPPING_PATH)
    partitioned_output_path: Path = Path(PARTITIONED_OUTPUT_PATH)
    single_output_path: Path = Path(SINGLE_OUTPUT_PATH)
    topic_output_path: Path = Path(TOPIC_OUTPUT_PATH)


DEFAULT_PIPELINE_PATHS = PipelinePaths()


def run_pipeline(paths: PipelinePaths = DEFAULT_PIPELINE_PATHS) -> None:
    spark = SparkSession.builder.appName("world-bank-data-pipeline").getOrCreate()
    long_dataframe: DataFrame | None = None
    topic_dataframe: DataFrame | None = None

    try:
        long_dataframe = read_world_bank_long_csv(spark, paths.api_indicator_output_path)
        topic_mapping = read_indicator_topic_mapping(spark, paths.mapping_path)
        long_dataframe = keep_only_countries_and_territories(
            keep_only_rows_with_values(long_dataframe),
        ).persist(StorageLevel.MEMORY_AND_DISK)
        write_long_csv(long_dataframe, paths.partitioned_output_path)
        topic_dataframe = add_topics_to_long_data(long_dataframe, topic_mapping).persist(
            StorageLevel.MEMORY_AND_DISK,
        )
        write_topic_csvs(topic_dataframe, paths.topic_output_path)
        write_topic_indicator_column_csvs(topic_dataframe, paths.topic_output_path)
        write_topic_wide_csvs(topic_dataframe, paths.topic_output_path)
        write_single_csv(long_dataframe, paths.single_output_path)
    finally:
        if topic_dataframe is not None:
            topic_dataframe.unpersist()

        if long_dataframe is not None:
            long_dataframe.unpersist()

        spark.stop()


def main(paths: PipelinePaths = DEFAULT_PIPELINE_PATHS) -> None:
    run_pipeline(paths)
