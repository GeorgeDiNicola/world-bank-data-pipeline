from collections.abc import Mapping, Sequence

from pyspark.sql import Column, DataFrame, Row
from pyspark.sql import functions as sf
from pyspark.sql.types import DataType, DoubleType, IntegerType, StringType

from world_bank_pipeline.config import (
    COUNTRY_CODE_COLUMN,
    COUNTRY_NAME_COLUMN,
    INDICATOR_COLUMN_ROW_COLUMNS,
    OUTPUT_COLUMNS,
    REQUIRED_WORLD_BANK_LONG_COLUMNS,
    REQUIRED_WORLD_BANK_TEXT_COLUMNS,
    SERIES_CODE_COLUMN,
    SERIES_NAME_COLUMN,
    TOPIC_COLUMN,
    VALUE_COLUMN,
    YEAR_COLUMN,
    YEAR_COLUMN_ROW_COLUMNS,
    YEAR_PATTERN,
)

MIN_REASONABLE_YEAR = 1800
MAX_REASONABLE_YEAR = 2100


def escape_spark_identifier(column_name: str) -> str:
    """Escape a column name for use in Spark SQL."""
    escaped_column_name = column_name.replace("`", "``")

    return f"`{escaped_column_name}`"


def require_columns(dataframe: DataFrame, required_columns: Sequence[str]) -> None:
    """Require a dataframe to include all expected columns."""
    missing_columns = [
        column_name for column_name in required_columns if column_name not in dataframe.columns
    ]

    if missing_columns:
        missing_column_names = ", ".join(missing_columns)
        raise ValueError(f"Input data is missing required columns: {missing_column_names}")


def get_trimmed_column_text(column_name: str) -> Column:
    """Return a string expression with nulls normalized to blank text."""
    return sf.trim(sf.coalesce(sf.col(column_name).cast("string"), sf.lit("")))


def get_try_cast_expression(column_name: str, target_type: str) -> Column:
    """Return a Spark SQL try_cast expression for a named column."""
    return sf.expr(
        f"try_cast(trim(cast({escape_spark_identifier(column_name)} as string)) "
        f"as {target_type})",
    )


def require_spark_column_types(
    dataframe: DataFrame,
    expected_types: Mapping[str, type[DataType]],
    dataset_name: str,
) -> None:
    """Require selected dataframe columns to have expected Spark data types."""
    require_columns(dataframe, list(expected_types))
    schema_by_name = {field.name: field.dataType for field in dataframe.schema.fields}
    invalid_columns: list[str] = []

    for column_name, expected_type in expected_types.items():
        actual_type = schema_by_name[column_name]

        if not isinstance(actual_type, expected_type):
            expected_type_name = expected_type().simpleString()
            actual_type_name = actual_type.simpleString()
            invalid_columns.append(
                f"{column_name} expected {expected_type_name}, found {actual_type_name}",
            )

    if invalid_columns:
        invalid_column_text = "; ".join(invalid_columns)
        raise ValueError(f"{dataset_name} has invalid column types: {invalid_column_text}.")


def _count_matching_rows(condition: Column) -> Column:
    return sf.coalesce(
        sf.sum(sf.when(condition, sf.lit(1)).otherwise(sf.lit(0))),
        sf.lit(0),
    )


def _get_summary_count(summary: Row, field_name: str) -> int:
    value = summary[field_name]

    if value is None:
        return 0

    return int(value)


def _missing_any_text_columns(columns: Sequence[str]) -> Column:
    missing_filter = get_trimmed_column_text(columns[0]) == ""

    for column_name in columns[1:]:
        missing_filter = missing_filter | (get_trimmed_column_text(column_name) == "")

    return missing_filter


def require_same_row_count(
    expected_row_count: int,
    actual_row_count: int,
    transformation_name: str,
) -> None:
    """Require a transformation to preserve row count when it should."""
    if actual_row_count != expected_row_count:
        raise ValueError(
            f"{transformation_name} changed row count from "
            f"{expected_row_count} to {actual_row_count}.",
        )


def validate_raw_world_bank_long_data(dataframe: DataFrame) -> int:
    require_columns(dataframe, REQUIRED_WORLD_BANK_LONG_COLUMNS)

    year_text = get_trimmed_column_text(YEAR_COLUMN)
    value_text = get_trimmed_column_text(VALUE_COLUMN)
    year_as_int = get_try_cast_expression(YEAR_COLUMN, "int")
    value_as_double = get_try_cast_expression(VALUE_COLUMN, "double")
    invalid_year = (
        ~year_text.rlike(YEAR_PATTERN)
        | year_as_int.isNull()
        | (year_as_int < MIN_REASONABLE_YEAR)
        | (year_as_int > MAX_REASONABLE_YEAR)
    )
    non_numeric_value = (value_text != "") & value_as_double.isNull()
    
    # single-pass aggregation to efficiently identify invalid data
    summary = dataframe.agg(
        sf.count(sf.lit(1)).alias("_row_count"),
        _count_matching_rows(
            _missing_any_text_columns(REQUIRED_WORLD_BANK_TEXT_COLUMNS),
        ).alias("_missing_identifier_count"),
        _count_matching_rows(invalid_year).alias("_invalid_year_count"),
        _count_matching_rows(non_numeric_value).alias("_non_numeric_value_count"),
    ).collect()[0]

    if _get_summary_count(summary, "_missing_identifier_count") > 0:
        raise ValueError(
            "World Bank data contains rows with missing country or series identifiers.",
        )

    if _get_summary_count(summary, "_invalid_year_count") > 0:
        raise ValueError("World Bank data contains rows with missing or invalid years.")

    if _get_summary_count(summary, "_non_numeric_value_count") > 0:
        raise ValueError("World Bank data contains non-numeric values.")

    return _get_summary_count(summary, "_row_count")


def validate_normalized_world_bank_long_data(
    dataframe: DataFrame,
    expected_row_count: int | None = None,
) -> int:
    """Validate long-form World Bank data after trimming and type normalization."""
    require_spark_column_types(
        dataframe,
        {
            COUNTRY_NAME_COLUMN: StringType,
            COUNTRY_CODE_COLUMN: StringType,
            SERIES_NAME_COLUMN: StringType,
            SERIES_CODE_COLUMN: StringType,
            YEAR_COLUMN: IntegerType,
            VALUE_COLUMN: DoubleType,
        },
        "Normalized World Bank data",
    )
    row_count = dataframe.count()

    if expected_row_count is not None:
        require_same_row_count(
            expected_row_count,
            row_count,
            "World Bank long data normalization",
        )

    return row_count


def validate_clean_world_bank_long_data(dataframe: DataFrame) -> int:
    """Validate cleaned long-form data after value and country filtering."""
    require_columns(dataframe, OUTPUT_COLUMNS)
    country_code_missing = get_trimmed_column_text(COUNTRY_CODE_COLUMN) == ""
    series_code_missing = get_trimmed_column_text(SERIES_CODE_COLUMN) == ""
    summary = dataframe.agg(
        sf.count(sf.lit(1)).alias("_row_count"),
        _count_matching_rows(country_code_missing).alias("_missing_country_code_count"),
        _count_matching_rows(series_code_missing).alias("_missing_series_code_count"),
        _count_matching_rows(sf.col(VALUE_COLUMN).isNull()).alias("_missing_value_count"),
    ).collect()[0]

    if _get_summary_count(summary, "_missing_country_code_count") > 0:
        raise ValueError("Cleaned World Bank data contains rows with missing country codes.")

    if _get_summary_count(summary, "_missing_series_code_count") > 0:
        raise ValueError("Cleaned World Bank data contains rows with missing series codes.")

    if _get_summary_count(summary, "_missing_value_count") > 0:
        raise ValueError("Cleaned World Bank data contains rows with missing values.")

    return _get_summary_count(summary, "_row_count")


def validate_topic_joined_long_data(dataframe: DataFrame) -> int:
    require_spark_column_types(
        dataframe,
        {
            COUNTRY_NAME_COLUMN: StringType,
            COUNTRY_CODE_COLUMN: StringType,
            SERIES_NAME_COLUMN: StringType,
            SERIES_CODE_COLUMN: StringType,
            YEAR_COLUMN: IntegerType,
            VALUE_COLUMN: DoubleType,
            TOPIC_COLUMN: StringType,
        },
        "Topic-joined World Bank data",
    )
    topic_missing = get_trimmed_column_text(TOPIC_COLUMN) == ""
    summary = dataframe.agg(
        sf.count(sf.lit(1)).alias("_row_count"),
        _count_matching_rows(topic_missing).alias("_missing_topic_count"),
    ).collect()[0]
    row_count = _get_summary_count(summary, "_row_count")

    if row_count == 0:
        raise ValueError("Final World Bank data is empty after topic mapping.")

    if _get_summary_count(summary, "_missing_topic_count") > 0:
        raise ValueError("Final World Bank data contains rows with missing topics.")

    require_unique_rows(
        dataframe,
        [COUNTRY_CODE_COLUMN, SERIES_CODE_COLUMN, YEAR_COLUMN],
        "final long",
    )

    return row_count


def require_unique_rows(dataframe: DataFrame, key_columns: Sequence[str], row_name: str) -> None:
    require_columns(dataframe, key_columns)
    duplicate_rows = (
        dataframe.groupBy(*key_columns)
        .count()
        .filter(sf.col("count") > 1)
        .limit(5)
        .collect()
    )

    if duplicate_rows:
        key_column_names = ", ".join(key_columns)
        examples = [
            ", ".join(f"{column_name}={row[column_name]!r}" for column_name in key_columns)
            for row in duplicate_rows
        ]
        example_text = "; ".join(examples)
        raise ValueError(
            f"Duplicate {row_name} rows found for: {key_column_names}. "
            f"Examples: {example_text}",
        )


def get_available_columns(dataframe: DataFrame, columns: Sequence[str]) -> list[str]:
    """Return expected columns that are present on a dataframe."""
    return [column_name for column_name in columns if column_name in dataframe.columns]


def get_indicator_column_row_columns(dataframe: DataFrame) -> list[str]:
    """Return row key columns for the indicator-wide output."""
    return get_available_columns(
        dataframe,
        [
            *INDICATOR_COLUMN_ROW_COLUMNS,
            TOPIC_COLUMN,
        ],
    )


def get_year_column_row_columns(dataframe: DataFrame) -> list[str]:
    """Return row key columns for the year-wide output."""
    return get_available_columns(
        dataframe,
        [
            *YEAR_COLUMN_ROW_COLUMNS,
            TOPIC_COLUMN,
        ],
    )


def require_unique_indicator_values(dataframe: DataFrame) -> None:
    """The later pivot to indicator columns requires unique indicator values for each country-year-topic cell."""
    indicator_value_columns = [
        *get_indicator_column_row_columns(dataframe),
        SERIES_NAME_COLUMN,
    ]

    require_unique_rows(
        dataframe,
        indicator_value_columns,
        "country-year-indicator",
    )


def require_unique_year_values(dataframe: DataFrame) -> None:
    """The later pivot to year columns requires unique year values for each country-indicator-topic cell."""
    year_value_columns = [
        *get_year_column_row_columns(dataframe),
        YEAR_COLUMN,
    ]

    require_unique_rows(
        dataframe,
        year_value_columns,
        "country-indicator-year",
    )
