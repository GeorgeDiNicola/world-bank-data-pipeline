from collections.abc import Iterator

import pytest

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from world_bank_pipeline.config import (
    COUNTRY_CODE_COLUMN,
    COUNTRY_NAME_COLUMN,
    OUTPUT_COLUMNS,
    SERIES_CODE_COLUMN,
    SERIES_NAME_COLUMN,
    TOPIC_COLUMN,
    VALUE_COLUMN,
    YEAR_COLUMN,
)
from world_bank_pipeline.validation import (
    require_columns,
    require_same_row_count,
    require_unique_indicator_values,
    require_unique_year_values,
    validate_clean_world_bank_long_data,
    validate_normalized_world_bank_long_data,
    validate_raw_world_bank_long_data,
    validate_topic_joined_long_data,
)


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Local SparkSession for validation tests."""
    spark_session = (
        SparkSession.builder.master("local[1]")
        .appName("world-bank-validation-tests")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


def get_topic_joined_schema() -> StructType:
    return StructType(
        [
            StructField(COUNTRY_NAME_COLUMN, StringType()),
            StructField(COUNTRY_CODE_COLUMN, StringType()),
            StructField(SERIES_NAME_COLUMN, StringType()),
            StructField(SERIES_CODE_COLUMN, StringType()),
            StructField(YEAR_COLUMN, IntegerType()),
            StructField(VALUE_COLUMN, DoubleType()),
            StructField(TOPIC_COLUMN, StringType()),
        ],
    )


def test_require_columns_rejects_missing_columns(spark: SparkSession) -> None:
    dataframe = spark.createDataFrame(
        [("Argentina", "ARG")],
        [COUNTRY_NAME_COLUMN, COUNTRY_CODE_COLUMN],
    )

    with pytest.raises(ValueError, match="missing required columns: Series Code"):
        require_columns(
            dataframe,
            [COUNTRY_NAME_COLUMN, COUNTRY_CODE_COLUMN, SERIES_CODE_COLUMN],
        )


def test_validate_raw_world_bank_long_data_accepts_valid_data(spark: SparkSession) -> None:
    dataframe = spark.createDataFrame(
        [
            (
                "Argentina",
                "ARG",
                "Adolescent fertility rate",
                "SP.ADO.TFRT",
                "2023",
                "26.414",
            ),
        ],
        OUTPUT_COLUMNS,
    )

    assert validate_raw_world_bank_long_data(dataframe) == 1


def test_validate_raw_world_bank_long_data_rejects_missing_required_columns(
    spark: SparkSession,
) -> None:
    dataframe = spark.createDataFrame(
        [("SP.ADO.TFRT", "Adolescent fertility rate", "ARG", "Argentina", 2023)],
        [
            SERIES_CODE_COLUMN,
            SERIES_NAME_COLUMN,
            COUNTRY_CODE_COLUMN,
            COUNTRY_NAME_COLUMN,
            YEAR_COLUMN,
        ],
    )

    with pytest.raises(ValueError, match="missing required columns: Value"):
        validate_raw_world_bank_long_data(dataframe)


def test_validate_raw_world_bank_long_data_rejects_invalid_years(
    spark: SparkSession,
) -> None:
    dataframe = spark.createDataFrame(
        [
            (
                "Argentina",
                "ARG",
                "Adolescent fertility rate",
                "SP.ADO.TFRT",
                "not-a-year",
                "26.414",
            ),
        ],
        OUTPUT_COLUMNS,
    )

    with pytest.raises(ValueError, match="invalid years"):
        validate_raw_world_bank_long_data(dataframe)


def test_validate_raw_world_bank_long_data_rejects_non_numeric_values(
    spark: SparkSession,
) -> None:
    dataframe = spark.createDataFrame(
        [
            (
                "Argentina",
                "ARG",
                "Adolescent fertility rate",
                "SP.ADO.TFRT",
                "2023",
                "not-a-number",
            ),
        ],
        OUTPUT_COLUMNS,
    )

    with pytest.raises(ValueError, match="non-numeric values"):
        validate_raw_world_bank_long_data(dataframe)


def test_validate_raw_world_bank_long_data_allows_null_values(
    spark: SparkSession,
) -> None:
    dataframe = spark.createDataFrame(
        [("SP.ADO.TFRT", "Adolescent fertility rate", "ARG", "Argentina", 2023, None)],
        StructType(
            [
                StructField(SERIES_CODE_COLUMN, StringType()),
                StructField(SERIES_NAME_COLUMN, StringType()),
                StructField(COUNTRY_CODE_COLUMN, StringType()),
                StructField(COUNTRY_NAME_COLUMN, StringType()),
                StructField(YEAR_COLUMN, IntegerType()),
                StructField(VALUE_COLUMN, DoubleType()),
            ],
        ),
    )

    assert validate_raw_world_bank_long_data(dataframe) == 1


def test_validate_raw_world_bank_long_data_rejects_missing_series_codes(
    spark: SparkSession,
) -> None:
    dataframe = spark.createDataFrame(
        [("Argentina", "ARG", "Adolescent fertility rate", "", "2023", "26.414")],
        OUTPUT_COLUMNS,
    )

    with pytest.raises(ValueError, match="missing country or series identifiers"):
        validate_raw_world_bank_long_data(dataframe)


def test_validate_normalized_world_bank_long_data_rejects_wrong_column_types(
    spark: SparkSession,
) -> None:
    schema = StructType(
        [
            StructField(COUNTRY_NAME_COLUMN, StringType()),
            StructField(COUNTRY_CODE_COLUMN, StringType()),
            StructField(SERIES_NAME_COLUMN, StringType()),
            StructField(SERIES_CODE_COLUMN, StringType()),
            StructField(YEAR_COLUMN, StringType()),
            StructField(VALUE_COLUMN, DoubleType()),
        ],
    )
    dataframe = spark.createDataFrame(
        [("Argentina", "ARG", "Adolescent fertility rate", "SP.ADO.TFRT", "2023", 26.414)],
        schema,
    )

    with pytest.raises(ValueError, match="invalid column types"):
        validate_normalized_world_bank_long_data(dataframe)


def test_validate_clean_world_bank_long_data_rejects_missing_country_codes(
    spark: SparkSession,
) -> None:
    dataframe = spark.createDataFrame(
        [("High income", "", "Adolescent fertility rate", "SP.ADO.TFRT", 2023, 26.414)],
        StructType(
            [
                StructField(COUNTRY_NAME_COLUMN, StringType()),
                StructField(COUNTRY_CODE_COLUMN, StringType()),
                StructField(SERIES_NAME_COLUMN, StringType()),
                StructField(SERIES_CODE_COLUMN, StringType()),
                StructField(YEAR_COLUMN, IntegerType()),
                StructField(VALUE_COLUMN, DoubleType()),
            ],
        ),
    )

    with pytest.raises(ValueError, match="missing country codes"):
        validate_clean_world_bank_long_data(dataframe)


def test_validate_topic_joined_long_data_accepts_valid_data(spark: SparkSession) -> None:
    dataframe = spark.createDataFrame(
        [
            (
                "Argentina",
                "ARG",
                "Adolescent fertility rate",
                "SP.ADO.TFRT",
                2023,
                26.414,
                "Health",
            ),
        ],
        get_topic_joined_schema(),
    )

    assert validate_topic_joined_long_data(dataframe) == 1


def test_validate_topic_joined_long_data_rejects_duplicate_keys(
    spark: SparkSession,
) -> None:
    dataframe = spark.createDataFrame(
        [
            (
                "Argentina",
                "ARG",
                "Adolescent fertility rate",
                "SP.ADO.TFRT",
                2023,
                26.414,
                "Health",
            ),
            (
                "Argentina",
                "ARG",
                "Adolescent fertility rate",
                "SP.ADO.TFRT",
                2023,
                27.0,
                "Health",
            ),
        ],
        get_topic_joined_schema(),
    )

    with pytest.raises(ValueError, match="Duplicate final long rows"):
        validate_topic_joined_long_data(dataframe)


def test_validate_topic_joined_long_data_rejects_empty_outputs(
    spark: SparkSession,
) -> None:
    dataframe = spark.createDataFrame([], get_topic_joined_schema())

    with pytest.raises(ValueError, match="empty after topic mapping"):
        validate_topic_joined_long_data(dataframe)


def test_require_unique_indicator_values_rejects_duplicate_cells(
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
        require_unique_indicator_values(dataframe)


def test_require_unique_year_values_rejects_duplicate_cells(
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
        require_unique_year_values(dataframe)


def test_require_same_row_count_rejects_changed_counts() -> None:
    with pytest.raises(ValueError, match="changed row count from 3 to 2"):
        require_same_row_count(3, 2, "Example transformation")
