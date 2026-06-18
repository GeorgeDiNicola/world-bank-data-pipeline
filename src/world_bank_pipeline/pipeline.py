from pyspark.sql import SparkSession

from world_bank_pipeline.io import (
    read_multiple_world_bank_csvs,
    write_long_csv,
    write_single_csv,
)
from world_bank_pipeline.transform import (
    convert_partitions_to_long_format,
    keep_only_rows_with_values,
)

INPUT_PATH = "/data/*.csv"
PARTITIONED_OUTPUT_PATH = "/output/world_bank_indicators_long"
SINGLE_OUTPUT_PATH = "/output/world_bank_indicators_long.csv"


def run_pipeline() -> None:
    spark = SparkSession.builder.appName("world-bank-wide-to-long").getOrCreate()

    try:
        wide_dataframes = read_multiple_world_bank_csvs(spark, INPUT_PATH)
        long_dataframe = convert_partitions_to_long_format(wide_dataframes)
        long_dataframe = keep_only_rows_with_values(long_dataframe)
        write_long_csv(long_dataframe, PARTITIONED_OUTPUT_PATH)
        write_single_csv(long_dataframe, SINGLE_OUTPUT_PATH)
    finally:
        spark.stop()


def main() -> None:
    run_pipeline()
