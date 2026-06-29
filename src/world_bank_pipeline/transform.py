from collections.abc import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as sf

from world_bank_pipeline.config import (
    COUNTRY_CODE_COLUMN,
    COUNTRY_NAME_COLUMN,
    INDICATOR_COLUMN_ROW_COLUMNS,
    MAPPING_TOPIC_COUNT_COLUMN,
    OUTPUT_COLUMNS,
    SERIES_CODE_COLUMN,
    SERIES_NAME_COLUMN,
    TOPIC_COLUMN,
    TOPIC_OUTPUT_COLUMNS,
    VALUE_COLUMN,
    WORLD_BANK_COUNTRY_CODES_EXCLUDE,
    YEAR_COLUMN,
    YEAR_COLUMN_ROW_COLUMNS,
)


def escape_spark_identifier(column_name: str) -> str:
    """Escape a column name for use in Spark SQL."""
    escaped_column_name = column_name.replace("`", "``")

    return f"`{escaped_column_name}`"


def keep_only_rows_with_values(dataframe: DataFrame) -> DataFrame:
    return dataframe.filter(sf.col(VALUE_COLUMN).isNotNull())


def keep_only_countries_and_territories(dataframe: DataFrame) -> DataFrame:
    """Exclude rows that are not countries or territories based on the World Bank country codes."""
    normalized_country_code = sf.upper(
        sf.trim(sf.coalesce(sf.col(COUNTRY_CODE_COLUMN), sf.lit(""))),
    )

    return dataframe.filter(
        (normalized_country_code != "")
        & ~normalized_country_code.isin(*WORLD_BANK_COUNTRY_CODES_EXCLUDE),
    )


def require_unique_rows(dataframe: DataFrame, key_columns: Sequence[str], row_name: str) -> None:
    duplicate_rows = (
        dataframe.groupBy(*key_columns)
        .count()
        .filter(sf.col("count") > 1)
        .limit(1)
    )

    if duplicate_rows.count() > 0:
        key_column_names = ", ".join(key_columns)
        raise ValueError(f"Duplicate {row_name} rows found for: {key_column_names}")


def get_available_columns(dataframe: DataFrame, columns: Sequence[str]) -> list[str]:
    return [column_name for column_name in columns if column_name in dataframe.columns]


def get_indicator_column_row_columns(dataframe: DataFrame) -> list[str]:
    return get_available_columns(
        dataframe,
        [
            *INDICATOR_COLUMN_ROW_COLUMNS,
            TOPIC_COLUMN,
        ],
    )


def get_year_column_row_columns(dataframe: DataFrame) -> list[str]:
    return get_available_columns(
        dataframe,
        [
            *YEAR_COLUMN_ROW_COLUMNS,
            TOPIC_COLUMN,
        ],
    )


def require_unique_indicator_values(dataframe: DataFrame) -> None:
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
    year_value_columns = [
        *get_year_column_row_columns(dataframe),
        YEAR_COLUMN,
    ]

    require_unique_rows(
        dataframe,
        year_value_columns,
        "country-indicator-year",
    )


def get_valid_topic_mapping(topic_mapping: DataFrame) -> DataFrame:
    mapping_columns = (
        topic_mapping.select(
            sf.trim(sf.col("id")).alias(SERIES_CODE_COLUMN),
            sf.trim(sf.col("topic")).alias(TOPIC_COLUMN),
        )
        .filter(
            sf.col(SERIES_CODE_COLUMN).isNotNull()
            & (sf.col(SERIES_CODE_COLUMN) != "")
            & sf.col(TOPIC_COLUMN).isNotNull()
            & (sf.col(TOPIC_COLUMN) != "")
        )
        .dropDuplicates([SERIES_CODE_COLUMN, TOPIC_COLUMN])
    )
    conflicting_mappings = (
        mapping_columns.groupBy(SERIES_CODE_COLUMN)
        .agg(sf.countDistinct(TOPIC_COLUMN).alias(MAPPING_TOPIC_COUNT_COLUMN))
        .filter(sf.col(MAPPING_TOPIC_COUNT_COLUMN) > 1)
        .limit(1)
    )

    if conflicting_mappings.count() > 0:
        raise ValueError("Topic mapping contains conflicting topics for the same series code.")

    return mapping_columns.dropDuplicates([SERIES_CODE_COLUMN])


def add_topics_to_long_data(dataframe: DataFrame, topic_mapping: DataFrame) -> DataFrame:
    mapping_columns = get_valid_topic_mapping(topic_mapping)
    normalized_dataframe = dataframe.withColumn(
        SERIES_CODE_COLUMN,
        sf.trim(sf.col(SERIES_CODE_COLUMN)),
    )

    return normalized_dataframe.join(
        sf.broadcast(mapping_columns),
        on=SERIES_CODE_COLUMN,
        how="inner",
    ).select(
        *OUTPUT_COLUMNS,
        TOPIC_COLUMN,
    )


def convert_long_to_indicator_columns(dataframe: DataFrame) -> DataFrame:
    """Return data with one country-year row and one column per indicator."""
    require_unique_indicator_values(dataframe)
    row_columns = get_indicator_column_row_columns(dataframe)

    series_names = [
        row[SERIES_NAME_COLUMN]
        for row in dataframe.select(SERIES_NAME_COLUMN)
        .distinct()
        .orderBy(SERIES_NAME_COLUMN)
        .collect()
    ]

    return (
        dataframe.groupBy(*row_columns)
        .pivot(SERIES_NAME_COLUMN, series_names)
        .agg(sf.first(VALUE_COLUMN))
        .select(
            *row_columns,
            *[sf.col(escape_spark_identifier(series_name)) for series_name in series_names],
        )
    )


def convert_long_to_year_columns(dataframe: DataFrame) -> DataFrame:
    """Return data with one country-indicator row and one column per year."""
    require_unique_year_values(dataframe)
    row_columns = get_year_column_row_columns(dataframe)

    years = [
        row[YEAR_COLUMN]
        for row in dataframe.select(YEAR_COLUMN).distinct().orderBy(YEAR_COLUMN).collect()
    ]

    return (
        dataframe.groupBy(*row_columns)
        .pivot(YEAR_COLUMN, years)
        .agg(sf.first(VALUE_COLUMN))
        .select(
            *row_columns,
            *[sf.col(escape_spark_identifier(str(year))) for year in years],
        )
    )
