from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession

from world_bank_pipeline.io import (
    read_indicator_topic_mapping,
    read_multiple_world_bank_csvs,
    write_long_csv,
    write_single_csv,
    write_topic_csvs,
)
from world_bank_pipeline.transform import (
    add_topics_to_long_data,
    convert_partitions_to_long_format,
    keep_only_rows_with_values,
)

INPUT_PATH = "/data/*.csv"
MAPPING_PATH = "/mapping/indicator_topic_mapping.csv"
PARTITIONED_OUTPUT_PATH = "/output/world_bank_indicators_long"
SINGLE_OUTPUT_PATH = "/output/world_bank_indicators_long.csv"
TOPIC_OUTPUT_PATH = "/output/world_bank_indicators_by_topic"


def run_pipeline() -> None:
    spark = SparkSession.builder.appName("world-bank-wide-to-long").getOrCreate()
    long_dataframe: DataFrame | None = None
    topic_dataframe: DataFrame | None = None

    try:
        wide_dataframes = read_multiple_world_bank_csvs(spark, INPUT_PATH)
        topic_mapping = read_indicator_topic_mapping(spark, MAPPING_PATH)
        long_dataframe = convert_partitions_to_long_format(wide_dataframes)
        long_dataframe = keep_only_rows_with_values(long_dataframe).persist(StorageLevel.MEMORY_AND_DISK)
        write_long_csv(long_dataframe, PARTITIONED_OUTPUT_PATH)
        topic_dataframe = add_topics_to_long_data(long_dataframe, topic_mapping).persist(
            StorageLevel.MEMORY_AND_DISK,
        )
        write_topic_csvs(topic_dataframe, TOPIC_OUTPUT_PATH)
        write_single_csv(long_dataframe, SINGLE_OUTPUT_PATH)
    finally:
        if topic_dataframe is not None:
            topic_dataframe.unpersist()

        if long_dataframe is not None:
            long_dataframe.unpersist()

        spark.stop()


def main() -> None:
    run_pipeline()
