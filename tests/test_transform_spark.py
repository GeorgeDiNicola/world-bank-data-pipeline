from collections.abc import Iterator
from pathlib import Path

import pytest

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from world_bank_pipeline.io import (
    read_world_bank_long_parquet,
    write_indicator_wide_parquet_dataset,
    write_inner_joined_indicator_topic_mapping_csv,
    write_long_parquet_dataset,
    write_year_wide_parquet_dataset,
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


def read_parquet_rows(
    spark: SparkSession,
    output_directory: Path,
) -> list[dict[str, object]]:
    return [row.asDict() for row in spark.read.parquet(str(output_directory)).collect()]


def read_csv_rows(spark: SparkSession, output_file: Path) -> list[dict[str, object]]:
    return [
        row.asDict()
        for row in spark.read.option("header", True).csv(str(output_file)).collect()
    ]


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


def test_read_world_bank_long_parquet_reads_api_output_as_long_dataframe(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "world_bank_api_indicator_data.parquet"
    spark.createDataFrame(
        [
            (" Argentina ", " ARG ", "Adolescent fertility rate", " SP.ADO.TFRT ", 2023, 26.414),
            ("Brazil", "BRA", "GDP", "NY.GDP.MKTP.CD", 2022, None),
        ],
        OUTPUT_COLUMNS,
    ).write.parquet(str(input_path))

    rows = read_world_bank_long_parquet(spark, input_path).orderBy("Country Code").collect()

    assert read_world_bank_long_parquet(spark, input_path).columns == OUTPUT_COLUMNS
    assert [(row["Country Code"], row["Year"], row["Value"]) for row in rows] == [
        ("ARG", 2023, 26.414),
        ("BRA", 2022, None),
    ]


def test_read_world_bank_long_parquet_rejects_missing_required_columns(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "world_bank_api_indicator_data.parquet"
    spark.createDataFrame(
        [("SP.ADO.TFRT", "Adolescent fertility rate", "ARG", "Argentina", 2023)],
        [
            SERIES_CODE_COLUMN,
            SERIES_NAME_COLUMN,
            COUNTRY_CODE_COLUMN,
            COUNTRY_NAME_COLUMN,
            "Year",
        ],
    ).write.parquet(str(input_path))

    with pytest.raises(ValueError, match="missing required columns: Value"):
        read_world_bank_long_parquet(spark, input_path)


def test_read_world_bank_long_parquet_rejects_invalid_years(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "world_bank_api_indicator_data.parquet"
    schema = StructType(
        [
            StructField(SERIES_CODE_COLUMN, StringType()),
            StructField(SERIES_NAME_COLUMN, StringType()),
            StructField(COUNTRY_CODE_COLUMN, StringType()),
            StructField(COUNTRY_NAME_COLUMN, StringType()),
            StructField("Year", StringType()),
            StructField("Value", DoubleType()),
        ],
    )
    spark.createDataFrame(
        [("SP.ADO.TFRT", "Adolescent fertility rate", "ARG", "Argentina", "not-a-year", 26.414)],
        schema,
    ).write.parquet(str(input_path))

    with pytest.raises(ValueError, match="invalid years"):
        read_world_bank_long_parquet(spark, input_path)


def test_read_world_bank_long_parquet_rejects_non_numeric_values(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "world_bank_api_indicator_data.parquet"
    schema = StructType(
        [
            StructField(SERIES_CODE_COLUMN, StringType()),
            StructField(SERIES_NAME_COLUMN, StringType()),
            StructField(COUNTRY_CODE_COLUMN, StringType()),
            StructField(COUNTRY_NAME_COLUMN, StringType()),
            StructField("Year", IntegerType()),
            StructField("Value", StringType()),
        ],
    )
    spark.createDataFrame(
        [("SP.ADO.TFRT", "Adolescent fertility rate", "ARG", "Argentina", 2023, "not-a-number")],
        schema,
    ).write.parquet(str(input_path))

    with pytest.raises(ValueError, match="non-numeric values"):
        read_world_bank_long_parquet(spark, input_path)


def test_read_world_bank_long_parquet_allows_null_values(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "world_bank_api_indicator_data.parquet"
    schema = StructType(
        [
            StructField(SERIES_CODE_COLUMN, StringType()),
            StructField(SERIES_NAME_COLUMN, StringType()),
            StructField(COUNTRY_CODE_COLUMN, StringType()),
            StructField(COUNTRY_NAME_COLUMN, StringType()),
            StructField("Year", IntegerType()),
            StructField("Value", DoubleType()),
        ],
    )
    spark.createDataFrame(
        [("SP.ADO.TFRT", "Adolescent fertility rate", "ARG", "Argentina", 2023, None)],
        schema,
    ).write.parquet(str(input_path))

    rows = read_world_bank_long_parquet(spark, input_path).collect()

    assert [(row["Year"], row["Value"]) for row in rows] == [(2023, None)]


def test_read_world_bank_long_parquet_rejects_missing_text_values(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "world_bank_api_indicator_data.parquet"
    spark.createDataFrame(
        [("Argentina", "ARG", "Adolescent fertility rate", "", 2023, 26.414)],
        OUTPUT_COLUMNS,
    ).write.parquet(str(input_path))

    with pytest.raises(ValueError, match="missing country or series identifiers"):
        read_world_bank_long_parquet(spark, input_path)


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


def test_convert_long_to_indicator_columns_preserves_topic_rows(
    spark: SparkSession,
) -> None:
    dataframe = spark.createDataFrame(
        [
            ("Argentina", "ARG", "GDP per capita", "NY.GDP.PCAP.CD", 2023, 2.0, "Economy"),
            ("Argentina", "ARG", "Population", "SP.POP.TOTL", 2023, 4.0, "Population"),
        ],
        [*OUTPUT_COLUMNS, TOPIC_COLUMN],
    )

    rows = convert_long_to_indicator_columns(dataframe).orderBy(TOPIC_COLUMN).collect()

    assert convert_long_to_indicator_columns(dataframe).columns == [
        COUNTRY_NAME_COLUMN,
        COUNTRY_CODE_COLUMN,
        "Year",
        TOPIC_COLUMN,
        "GDP per capita",
        "Population",
    ]
    assert [
        (row[TOPIC_COLUMN], row["GDP per capita"], row["Population"])
        for row in rows
    ] == [
        ("Economy", 2.0, None),
        ("Population", None, 4.0),
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


def test_convert_long_to_year_columns_preserves_topic_rows(spark: SparkSession) -> None:
    dataframe = spark.createDataFrame(
        [
            ("Argentina", "ARG", "GDP per capita", "NY.GDP.PCAP.CD", 2022, 1.0, "Economy"),
            ("Argentina", "ARG", "GDP per capita", "NY.GDP.PCAP.CD", 2023, 2.0, "Economy"),
        ],
        [*OUTPUT_COLUMNS, TOPIC_COLUMN],
    )

    rows = convert_long_to_year_columns(dataframe).orderBy("Country Code").collect()

    assert convert_long_to_year_columns(dataframe).columns == [
        COUNTRY_NAME_COLUMN,
        COUNTRY_CODE_COLUMN,
        SERIES_NAME_COLUMN,
        SERIES_CODE_COLUMN,
        TOPIC_COLUMN,
        "2022",
        "2023",
    ]
    assert [(row[TOPIC_COLUMN], row["2022"], row["2023"]) for row in rows] == [
        ("Economy", 1.0, 2.0),
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


def test_write_long_parquet_dataset_creates_topic_joined_dataset(
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

    output_directory = tmp_path / "world_bank_indicators_long.parquet"

    write_long_parquet_dataset(dataframe, output_directory)

    assert list(output_directory.glob("part-*.parquet"))
    assert sorted(
        read_parquet_rows(spark, output_directory),
        key=lambda row: str(row["Country Code"]),
    ) == [
        {
            "Country Name": "Afghanistan",
            "Country Code": "AFG",
            "Series Name": "Health indicator",
            "Series Code": "SP.ADO.TFRT",
            "Year": 2024,
            "Value": 1.5,
            "Topic": "Health",
        },
        {
            "Country Name": "Zimbabwe",
            "Country Code": "ZWE",
            "Series Name": "Agriculture indicator",
            "Series Code": "NV.AGR.TOTL.ZS",
            "Year": 2024,
            "Value": 2.0,
            "Topic": "Agriculture & Rural Development",
        },
    ]


def test_write_inner_joined_indicator_topic_mapping_csv_filters_final_indicators(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    topic_mapping = spark.createDataFrame(
        [
            (
                "SP.ADO.TFRT",
                "Health indicator",
                "2",
                "World Development Indicators",
                "World Bank",
                "Health",
            ),
            (
                "NV.AGR.TOTL.ZS",
                "Agriculture indicator",
                "2",
                "World Development Indicators",
                "World Bank",
                "Agriculture & Rural Development",
            ),
            (
                "UNUSED",
                "Unused indicator",
                "2",
                "World Development Indicators",
                "World Bank",
                "Unused",
            ),
        ],
        ["id", "name", "source_id", "source", "source_organization", "topic"],
    )
    final_long_dataframe = spark.createDataFrame(
        [
            ("Argentina", "ARG", "Health indicator", "SP.ADO.TFRT", 2023, 1.5, "Health"),
            (
                "Zimbabwe",
                "ZWE",
                "Agriculture indicator",
                "NV.AGR.TOTL.ZS",
                2024,
                3.0,
                "Agriculture & Rural Development",
            ),
        ],
        [*OUTPUT_COLUMNS, TOPIC_COLUMN],
    )
    output_file = tmp_path / "indicator_topic_mapping.csv"

    write_inner_joined_indicator_topic_mapping_csv(
        topic_mapping,
        final_long_dataframe,
        output_file,
    )

    rows = sorted(read_csv_rows(spark, output_file), key=lambda row: str(row["id"]))

    assert rows == [
        {
            "id": "NV.AGR.TOTL.ZS",
            "name": "Agriculture indicator",
            "source_id": "2",
            "source": "World Development Indicators",
            "source_organization": "World Bank",
            "topic": "Agriculture & Rural Development",
        },
        {
            "id": "SP.ADO.TFRT",
            "name": "Health indicator",
            "source_id": "2",
            "source": "World Development Indicators",
            "source_organization": "World Bank",
            "topic": "Health",
        },
    ]


def test_write_indicator_wide_parquet_dataset_creates_topic_joined_dataset(
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

    output_directory = tmp_path / "world_bank_indicators_indicator_wide.parquet"

    write_indicator_wide_parquet_dataset(dataframe, output_directory)

    assert list(output_directory.glob("part-*.parquet"))
    assert sorted(
        read_parquet_rows(spark, output_directory),
        key=lambda row: (str(row["Country Code"]), int(row["Year"]), str(row[TOPIC_COLUMN])),
    ) == [
        {
            "Country Name": "Afghanistan",
            "Country Code": "AFG",
            "Year": 2023,
            "Topic": "Health",
            "Agriculture indicator": None,
            "Health indicator": 1.0,
            "Mortality indicator": None,
        },
        {
            "Country Name": "Afghanistan",
            "Country Code": "AFG",
            "Year": 2024,
            "Topic": "Health",
            "Agriculture indicator": None,
            "Health indicator": None,
            "Mortality indicator": 3.5,
        },
        {
            "Country Name": "Zimbabwe",
            "Country Code": "ZWE",
            "Year": 2024,
            "Topic": "Agriculture & Rural Development",
            "Agriculture indicator": 2.0,
            "Health indicator": None,
            "Mortality indicator": None,
        },
    ]


def test_write_year_wide_parquet_dataset_creates_topic_joined_dataset(
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

    output_directory = tmp_path / "world_bank_indicators_year_wide.parquet"

    write_year_wide_parquet_dataset(dataframe, output_directory)

    assert list(output_directory.glob("part-*.parquet"))
    assert sorted(
        read_parquet_rows(spark, output_directory),
        key=lambda row: str(row["Country Code"]),
    ) == [
        {
            "Country Name": "Afghanistan",
            "Country Code": "AFG",
            "Series Name": "Health indicator",
            "Series Code": "SP.ADO.TFRT",
            "Topic": "Health",
            "2023": 1.0,
            "2024": 1.5,
        },
        {
            "Country Name": "Zimbabwe",
            "Country Code": "ZWE",
            "Series Name": "Agriculture indicator",
            "Series Code": "NV.AGR.TOTL.ZS",
            "Topic": "Agriculture & Rural Development",
            "2023": None,
            "2024": 2.0,
        },
    ]
