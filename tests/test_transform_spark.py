from collections.abc import Iterator

import pytest

from pyspark.sql import SparkSession

from src.world_bank_pipeline.transform import (
    OUTPUT_COLUMNS,
    convert_partitions_to_long_format,
    convert_wide_to_long,
    keep_only_rows_with_all_identifiers,
    keep_only_rows_with_values,
)


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Local SparkSession for testing."""
    spark_session = (
        SparkSession.builder.master("local[1]")
        .appName("world-bank-pipeline-tests")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


def test_keep_only_rows_with_all_identifiers_removes_footer_rows(spark: SparkSession) -> None:
    """Footer and blank rows are removed before reshaping."""
    dataframe = spark.createDataFrame(
        [
            ("Afghanistan", "AFG", "Indicator", "TEST.CODE", "1"),
            ("Data from database: World Development Indicators", "", "", "", ""),
            ("Last Updated: 04/08/2026", "", "", "", ""),
            ("", "", "", "", ""),
        ],
        ["Country Name", "Country Code", "Series Name", "Series Code", "2024 [YR2024]"],
    )

    rows = keep_only_rows_with_all_identifiers(dataframe).collect()

    assert len(rows) == 1
    assert rows[0]["Country Name"] == "Afghanistan"


def test_convert_wide_to_long_outputs_expected_rows(spark: SparkSession) -> None:
    dataframe = spark.createDataFrame(
        [
            ("Afghanistan", "AFG", "Indicator", "TEST.CODE", "1.5", ".."),
            ("Zimbabwe", "ZWE", "Indicator", "TEST.CODE", "", "2"),
            ("Last Updated: 04/08/2026", "", "", "", "", ""),
        ],
        [
            "Country Name",
            "Country Code",
            "Series Name",
            "Series Code",
            "2024 [YR2024]",
            "2025 [YR2025]",
        ],
    )

    rows = convert_wide_to_long(dataframe).orderBy("Country Name", "Year").collect()

    assert convert_wide_to_long(dataframe).columns == OUTPUT_COLUMNS
    assert [(row["Country Code"], row["Year"], row["Value"]) for row in rows] == [
        ("AFG", 2024, 1.5),
        ("AFG", 2025, None),
        ("ZWE", 2024, None),
        ("ZWE", 2025, 2.0),
    ]


def test_keep_only_rows_with_values_removes_missing_values(spark: SparkSession) -> None:
    dataframe = spark.createDataFrame(
        [
            ("Afghanistan", "AFG", "Indicator", "TEST.CODE", 2024, 1.5),
            ("Afghanistan", "AFG", "Indicator", "TEST.CODE", 2025, None),
            ("Zimbabwe", "ZWE", "Indicator", "TEST.CODE", 2024, 2.0),
        ],
        OUTPUT_COLUMNS,
    )

    rows = keep_only_rows_with_values(dataframe).orderBy("Country Code").collect()

    assert [(row["Country Code"], row["Year"], row["Value"]) for row in rows] == [
        ("AFG", 2024, 1.5),
        ("ZWE", 2024, 2.0),
    ]


def test_convert_partitions_to_long_unions_partition_years(spark: SparkSession) -> None:
    first_dataframe = spark.createDataFrame(
        [("Afghanistan", "AFG", "Indicator", "TEST.CODE", "1")],
        ["Country Name", "Country Code", "Series Name", "Series Code", "2024 [YR2024]"],
    )
    second_dataframe = spark.createDataFrame(
        [("Afghanistan", "AFG", "Indicator", "TEST.CODE", "2")],
        ["Country Name", "Country Code", "Series Name", "Series Code", "2025 [YR2025]"],
    )

    rows = convert_partitions_to_long_format([second_dataframe, first_dataframe]).collect()

    assert [(row["Year"], row["Value"]) for row in rows] == [(2024, 1.0), (2025, 2.0)]
