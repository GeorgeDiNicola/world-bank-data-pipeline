import re
from collections.abc import Sequence
from pyspark.sql import DataFrame
from pyspark.sql import functions as sf
from pyspark.sql.types import DoubleType, IntegerType


COUNTRY_NAME_COLUMN = "Country Name"
COUNTRY_CODE_COLUMN = "Country Code"
SERIES_NAME_COLUMN = "Series Name"
SERIES_CODE_COLUMN = "Series Code"
VALUE_COLUMN = "Value"
YEAR_COLUMN = "Year"

IDENTIFIER_COLUMNS = [
    COUNTRY_NAME_COLUMN,
    COUNTRY_CODE_COLUMN,
    SERIES_NAME_COLUMN,
    SERIES_CODE_COLUMN,
]
OUTPUT_COLUMNS = [
    COUNTRY_NAME_COLUMN,
    COUNTRY_CODE_COLUMN,
    SERIES_NAME_COLUMN,
    SERIES_CODE_COLUMN,
    YEAR_COLUMN,
    VALUE_COLUMN,
]
YEAR_COLUMN_PATTERN = re.compile(r"^(?P<year>\d{4}) \[YR(?P=year)\]$")


def get_year_columns(columns: Sequence[str]) -> list[tuple[str, int]]:
    """Return World Bank year columns paired with their integer year."""
    year_columns: list[tuple[str, int]] = []

    for column_name in columns:
        match = YEAR_COLUMN_PATTERN.match(column_name)
        if match is not None:
            year_columns.append((column_name, int(match.group("year"))))

    return sorted(year_columns, key=lambda column: column[1])


def validate_input_columns(columns: Sequence[str], year_columns: Sequence[tuple[str, int]]) -> None:
    missing_identifier_columns = [
        column_name for column_name in IDENTIFIER_COLUMNS if column_name not in columns
    ]

    if missing_identifier_columns:
        missing_columns = ", ".join(missing_identifier_columns)
        raise ValueError(f"Input data is missing required columns: {missing_columns}")

    if not year_columns:
        raise ValueError("Input data does not include any columns like '2024 [YR2024]'.")


def keep_only_rows_with_all_identifiers(dataframe: DataFrame) -> DataFrame:
    required_identifier_values = [
        sf.trim(sf.col(column_name)).isNotNull()
        & (sf.trim(sf.col(column_name)) != "")
        for column_name in IDENTIFIER_COLUMNS
    ]
    complete_identifier_filter = required_identifier_values[0]

    for next_filter in required_identifier_values[1:]:
        complete_identifier_filter = complete_identifier_filter & next_filter

    return dataframe.filter(complete_identifier_filter)


def keep_only_rows_with_values(dataframe: DataFrame) -> DataFrame:
    return dataframe.filter(sf.col(VALUE_COLUMN).isNotNull())


def convert_wide_to_long(dataframe: DataFrame) -> DataFrame:
    year_columns = get_year_columns(dataframe.columns)
    validate_input_columns(dataframe.columns, year_columns)
    data_rows = keep_only_rows_with_all_identifiers(dataframe)

    unpivoted_dataframes = [
        data_rows.select(
            *[sf.col(column_name) for column_name in IDENTIFIER_COLUMNS],
            sf.lit(year).cast(IntegerType()).alias(YEAR_COLUMN),
            sf.when(
                sf.trim(sf.col(column_name)).isin("", ".."),
                sf.lit(None),
            )
            .otherwise(sf.trim(sf.col(column_name)).cast(DoubleType()))
            .alias(VALUE_COLUMN),
        )
        for column_name, year in year_columns
    ]

    first_dataframe, *remaining_dataframes = unpivoted_dataframes
    long_dataframe = first_dataframe

    for next_dataframe in remaining_dataframes:
        long_dataframe = long_dataframe.unionByName(next_dataframe)

    return long_dataframe.select(*OUTPUT_COLUMNS)


def convert_partitions_to_long_format(dataframes: Sequence[DataFrame]) -> DataFrame:
    if not dataframes:
        raise ValueError("At least one input dataframe is required.")

    long_dataframes = [convert_wide_to_long(dataframe) for dataframe in dataframes]
    first_dataframe, *remaining_dataframes = long_dataframes
    long_dataframe = first_dataframe

    for next_dataframe in remaining_dataframes:
        long_dataframe = long_dataframe.unionByName(next_dataframe)

    return long_dataframe.orderBy(
        COUNTRY_NAME_COLUMN,
        COUNTRY_CODE_COLUMN,
        SERIES_NAME_COLUMN,
        SERIES_CODE_COLUMN,
        YEAR_COLUMN,
    )
