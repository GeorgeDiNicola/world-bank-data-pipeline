import csv
from collections.abc import Iterator
from pathlib import Path

import pytest

from pyspark.sql import SparkSession

from world_bank_pipeline.io import (
    read_world_bank_long_csv,
    write_topic_indicator_column_csvs,
    write_topic_csvs,
    write_topic_wide_csvs,
)
from world_bank_pipeline.transform import (
    COUNTRY_CODE_COLUMN,
    COUNTRY_NAME_COLUMN,
    SERIES_CODE_COLUMN,
    SERIES_NAME_COLUMN,
    OUTPUT_COLUMNS,
    TOPIC_COLUMN,
    add_topics_to_long_data,
    convert_long_to_indicator_columns,
    convert_long_to_year_columns,
    keep_only_countries_and_territories,
    keep_only_rows_with_values,
)


def read_csv_rows(output_file: Path) -> list[dict[str, str]]:
    with output_file.open(newline="", encoding="utf-8") as csv_file:
        return list(csv.DictReader(csv_file))


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


def test_keep_only_countries_and_territories_removes_aggregate_economies(
    spark: SparkSession,
) -> None:
    dataframe = spark.createDataFrame(
        [
            ("Afghanistan", "AFG", "Indicator", "TEST.CODE", 2024, 1.0),
            ("Aruba", "ABW", "Indicator", "TEST.CODE", 2024, 2.0),
            ("Kosovo", "XKX", "Indicator", "TEST.CODE", 2024, 3.0),
            ("World", "WLD", "Indicator", "TEST.CODE", 2024, 4.0),
            ("North America", "NAC", "Indicator", "TEST.CODE", 2024, 5.0),
            ("High income", "", "Indicator", "TEST.CODE", 2024, 6.0),
            ("Upper middle income", None, "Indicator", "TEST.CODE", 2024, 7.0),
        ],
        OUTPUT_COLUMNS,
    )

    rows = (
        keep_only_countries_and_territories(dataframe)
        .orderBy(COUNTRY_CODE_COLUMN)
        .collect()
    )

    assert [(row[COUNTRY_CODE_COLUMN], row[COUNTRY_NAME_COLUMN]) for row in rows] == [
        ("ABW", "Aruba"),
        ("AFG", "Afghanistan"),
        ("XKX", "Kosovo"),
    ]


def test_read_world_bank_long_csv_reads_api_output_as_long_dataframe(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    input_file = tmp_path / "world_bank_api_indicator_data.csv"
    input_file.write_text(
        "Series Code,Series Name,Country Code,Country Name,Year,Value\n"
        " SP.ADO.TFRT ,Adolescent fertility rate, ARG , Argentina ,2023,26.414\n"
        "NY.GDP.MKTP.CD,GDP,BRA,Brazil,2022,\n",
        encoding="utf-8",
    )

    rows = read_world_bank_long_csv(spark, str(input_file)).orderBy("Country Code").collect()

    assert read_world_bank_long_csv(spark, str(input_file)).columns == OUTPUT_COLUMNS
    assert [(row["Country Code"], row["Year"], row["Value"]) for row in rows] == [
        ("ARG", 2023, 26.414),
        ("BRA", 2022, None),
    ]


def test_read_world_bank_long_csv_rejects_missing_required_columns(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    input_file = tmp_path / "world_bank_api_indicator_data.csv"
    input_file.write_text(
        "Series Code,Series Name,Country Code,Country Name,Year\n"
        "SP.ADO.TFRT,Adolescent fertility rate,ARG,Argentina,2023\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="missing required columns: Value"):
        read_world_bank_long_csv(spark, input_file)


def test_read_world_bank_long_csv_rejects_invalid_years(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    input_file = tmp_path / "world_bank_api_indicator_data.csv"
    input_file.write_text(
        "Series Code,Series Name,Country Code,Country Name,Year,Value\n"
        "SP.ADO.TFRT,Adolescent fertility rate,ARG,Argentina,not-a-year,26.414\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="invalid years"):
        read_world_bank_long_csv(spark, input_file)


def test_read_world_bank_long_csv_rejects_non_numeric_values(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    input_file = tmp_path / "world_bank_api_indicator_data.csv"
    input_file.write_text(
        "Series Code,Series Name,Country Code,Country Name,Year,Value\n"
        "SP.ADO.TFRT,Adolescent fertility rate,ARG,Argentina,2023,not-a-number\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="non-numeric values"):
        read_world_bank_long_csv(spark, input_file)


def test_add_topics_to_long_data_maps_indicators_to_topics(spark: SparkSession) -> None:
    dataframe = spark.createDataFrame(
        [
            ("Afghanistan", "AFG", "Health indicator", " SP.ADO.TFRT ", 2024, 1.5),
            ("Zimbabwe", "ZWE", "Agriculture indicator", "NV.AGR.TOTL.ZS", 2024, 2.0),
            ("Canada", "CAN", "Unmapped indicator", "UNMAPPED", 2024, 3.0),
        ],
        OUTPUT_COLUMNS,
    )
    topic_mapping = spark.createDataFrame(
        [
            ("SP.ADO.TFRT", "Health"),
            ("NV.AGR.TOTL.ZS", "Agriculture & Rural Development"),
        ],
        ["id", "topic"],
    )

    rows = add_topics_to_long_data(dataframe, topic_mapping).orderBy(TOPIC_COLUMN).collect()

    assert add_topics_to_long_data(dataframe, topic_mapping).columns == [
        *OUTPUT_COLUMNS,
        TOPIC_COLUMN,
    ]
    assert [(row["Series Code"], row[TOPIC_COLUMN]) for row in rows] == [
        ("NV.AGR.TOTL.ZS", "Agriculture & Rural Development"),
        ("SP.ADO.TFRT", "Health"),
    ]


def test_add_topics_to_long_data_rejects_conflicting_topic_mappings(
    spark: SparkSession,
) -> None:
    dataframe = spark.createDataFrame(
        [("Afghanistan", "AFG", "Health indicator", "SP.ADO.TFRT", 2024, 1.5)],
        OUTPUT_COLUMNS,
    )
    topic_mapping = spark.createDataFrame(
        [
            ("SP.ADO.TFRT", "Health"),
            ("SP.ADO.TFRT", "Education"),
        ],
        ["id", "topic"],
    )

    with pytest.raises(ValueError, match="conflicting topics"):
        add_topics_to_long_data(dataframe, topic_mapping)


def test_convert_long_to_indicator_columns_uses_indicator_columns(spark: SparkSession) -> None:
    dataframe = spark.createDataFrame(
        [
            ("Argentina", "ARG", "GDP per capita", "NY.GDP.PCAP.CD", 2022, 1.0),
            ("Argentina", "ARG", "GDP per capita", "NY.GDP.PCAP.CD", 2023, 2.0),
            ("Argentina", "ARG", "Population", "SP.POP.TOTL", 2023, 4.0),
            ("Brazil", "BRA", "GDP per capita", "NY.GDP.PCAP.CD", 2023, 3.0),
        ],
        OUTPUT_COLUMNS,
    )

    rows = convert_long_to_indicator_columns(dataframe).orderBy("Country Code", "Year").collect()

    assert convert_long_to_indicator_columns(dataframe).columns == [
        COUNTRY_NAME_COLUMN,
        COUNTRY_CODE_COLUMN,
        "Year",
        "GDP per capita",
        "Population",
    ]
    assert [
        (row["Country Code"], row["Year"], row["GDP per capita"], row["Population"])
        for row in rows
    ] == [
        ("ARG", 2022, 1.0, None),
        ("ARG", 2023, 2.0, 4.0),
        ("BRA", 2023, 3.0, None),
    ]


def test_convert_long_to_indicator_columns_rejects_duplicate_output_cells(
    spark: SparkSession,
) -> None:
    dataframe = spark.createDataFrame(
        [
            ("Argentina", "ARG", "GDP per capita", "NY.GDP.PCAP.CD", 2023, 2.0),
            ("Argentina", "ARG", "GDP per capita", "NY.GDP.PCAP.CD", 2023, 3.0),
        ],
        OUTPUT_COLUMNS,
    )

    with pytest.raises(ValueError, match="Duplicate country-year-indicator rows"):
        convert_long_to_indicator_columns(dataframe)


def test_convert_long_to_year_columns_uses_year_columns(spark: SparkSession) -> None:
    dataframe = spark.createDataFrame(
        [
            ("Argentina", "ARG", "GDP per capita", "NY.GDP.PCAP.CD", 2022, 1.0),
            ("Argentina", "ARG", "GDP per capita", "NY.GDP.PCAP.CD", 2023, 2.0),
            ("Brazil", "BRA", "GDP per capita", "NY.GDP.PCAP.CD", 2023, 3.0),
        ],
        OUTPUT_COLUMNS,
    )

    rows = convert_long_to_year_columns(dataframe).orderBy("Country Code").collect()

    assert convert_long_to_year_columns(dataframe).columns == [
        COUNTRY_NAME_COLUMN,
        COUNTRY_CODE_COLUMN,
        SERIES_NAME_COLUMN,
        SERIES_CODE_COLUMN,
        "2022",
        "2023",
    ]
    assert [(row["Country Code"], row["2022"], row["2023"]) for row in rows] == [
        ("ARG", 1.0, 2.0),
        ("BRA", None, 3.0),
    ]


def test_convert_long_to_year_columns_rejects_duplicate_output_cells(
    spark: SparkSession,
) -> None:
    dataframe = spark.createDataFrame(
        [
            ("Argentina", "ARG", "GDP per capita", "NY.GDP.PCAP.CD", 2023, 2.0),
            ("Argentina", "ARG", "GDP per capita", "NY.GDP.PCAP.CD", 2023, 3.0),
        ],
        OUTPUT_COLUMNS,
    )

    with pytest.raises(ValueError, match="Duplicate country-indicator-year rows"):
        convert_long_to_year_columns(dataframe)


def test_write_topic_csvs_creates_one_csv_file_per_topic(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    dataframe = spark.createDataFrame(
        [
            (
                "Afghanistan",
                "AFG",
                "Health indicator",
                "SP.ADO.TFRT",
                2024,
                1.5,
                "Health",
            ),
            (
                "Zimbabwe",
                "ZWE",
                "Agriculture indicator",
                "NV.AGR.TOTL.ZS",
                2024,
                2.0,
                "Agriculture & Rural Development",
            ),
        ],
        [*OUTPUT_COLUMNS, TOPIC_COLUMN],
    )

    output_directory = tmp_path / "topics"
    stale_output = output_directory / "stale_long.csv"
    output_directory.mkdir()
    stale_output.write_text("stale\n")

    write_topic_csvs(dataframe, str(output_directory))

    health_output = output_directory / "health_long.csv"
    agriculture_output = output_directory / "agriculture_rural_development_long.csv"
    assert health_output.exists()
    assert agriculture_output.exists()
    assert not stale_output.exists()
    assert sorted(read_csv_rows(health_output), key=lambda row: row["Year"]) == [
        {
            "Country Name": "Afghanistan",
            "Country Code": "AFG",
            "Series Name": "Health indicator",
            "Series Code": "SP.ADO.TFRT",
            "Year": "2024",
            "Value": "1.5",
        },
    ]


def test_write_topic_indicator_column_csvs_creates_one_csv_file_per_topic(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    dataframe = spark.createDataFrame(
        [
            (
                "Afghanistan",
                "AFG",
                "Health indicator",
                "SP.ADO.TFRT",
                2023,
                1.0,
                "Health",
            ),
            (
                "Afghanistan",
                "AFG",
                "Mortality indicator",
                "SH.DYN.MORT",
                2024,
                3.5,
                "Health",
            ),
            (
                "Zimbabwe",
                "ZWE",
                "Agriculture indicator",
                "NV.AGR.TOTL.ZS",
                2024,
                2.0,
                "Agriculture & Rural Development",
            ),
        ],
        [*OUTPUT_COLUMNS, TOPIC_COLUMN],
    )

    output_directory = tmp_path / "topics"
    stale_output = output_directory / "stale.csv"
    output_directory.mkdir()
    stale_output.write_text("stale\n")

    write_topic_indicator_column_csvs(dataframe, str(output_directory))

    health_output = output_directory / "health.csv"
    agriculture_output = output_directory / "agriculture_rural_development.csv"
    assert health_output.exists()
    assert agriculture_output.exists()
    assert not stale_output.exists()
    assert sorted(read_csv_rows(health_output), key=lambda row: row["Year"]) == [
        {
            "Country Name": "Afghanistan",
            "Country Code": "AFG",
            "Year": "2023",
            "Health indicator": "1.0",
            "Mortality indicator": "",
        },
        {
            "Country Name": "Afghanistan",
            "Country Code": "AFG",
            "Year": "2024",
            "Health indicator": "",
            "Mortality indicator": "3.5",
        },
    ]


def test_write_topic_wide_csvs_creates_one_csv_file_per_topic(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    dataframe = spark.createDataFrame(
        [
            (
                "Afghanistan",
                "AFG",
                "Health indicator",
                "SP.ADO.TFRT",
                2023,
                1.0,
                "Health",
            ),
            (
                "Afghanistan",
                "AFG",
                "Health indicator",
                "SP.ADO.TFRT",
                2024,
                1.5,
                "Health",
            ),
            (
                "Zimbabwe",
                "ZWE",
                "Agriculture indicator",
                "NV.AGR.TOTL.ZS",
                2024,
                2.0,
                "Agriculture & Rural Development",
            ),
        ],
        [*OUTPUT_COLUMNS, TOPIC_COLUMN],
    )

    output_directory = tmp_path / "topics"
    stale_output = output_directory / "stale_wide.csv"
    output_directory.mkdir()
    stale_output.write_text("stale\n")

    write_topic_wide_csvs(dataframe, str(output_directory))

    health_output = output_directory / "health_wide.csv"
    agriculture_output = output_directory / "agriculture_rural_development_wide.csv"
    assert health_output.exists()
    assert agriculture_output.exists()
    assert not stale_output.exists()
    assert read_csv_rows(health_output) == [
        {
            "Country Name": "Afghanistan",
            "Country Code": "AFG",
            "Series Name": "Health indicator",
            "Series Code": "SP.ADO.TFRT",
            "2023": "1.0",
            "2024": "1.5",
        },
    ]


def test_write_topic_csvs_rejects_topic_output_path_collisions(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    dataframe = spark.createDataFrame(
        [
            ("Afghanistan", "AFG", "Indicator", "TEST.1", 2024, 1.0, "A&B"),
            ("Zimbabwe", "ZWE", "Indicator", "TEST.2", 2024, 2.0, "A B"),
        ],
        [*OUTPUT_COLUMNS, TOPIC_COLUMN],
    )

    with pytest.raises(ValueError, match="produce the same output file"):
        write_topic_csvs(dataframe, str(tmp_path / "topics"))
