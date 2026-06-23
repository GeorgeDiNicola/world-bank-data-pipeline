import re
import shutil
from collections.abc import Callable, Sequence
from pathlib import Path
from uuid import uuid4

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.types import IntegerType

from world_bank_pipeline.transform import (
    COUNTRY_NAME_COLUMN,
    OUTPUT_COLUMNS,
    SERIES_CODE_COLUMN,
    SERIES_NAME_COLUMN,
    TOPIC_COLUMN,
    VALUE_COLUMN,
    YEAR_COLUMN,
    convert_long_to_indicator_columns,
    convert_long_to_year_columns,
    escape_spark_identifier,
)

NON_ALPHANUMERIC_PATTERN = re.compile(r"[^a-z0-9]+")
YEAR_PATTERN = r"^\d{4}$"
REQUIRED_WORLD_BANK_LONG_COLUMNS = OUTPUT_COLUMNS
REQUIRED_TOPIC_MAPPING_COLUMNS = ["id", "topic"]
REQUIRED_WORLD_BANK_TEXT_COLUMNS = [
    COUNTRY_NAME_COLUMN,
    SERIES_NAME_COLUMN,
    SERIES_CODE_COLUMN,
]

PathInput = str | Path
TopicPathGetter = Callable[[Path, str], Path]


def require_columns(dataframe: DataFrame, required_columns: Sequence[str]) -> None:
    missing_columns = [
        column_name for column_name in required_columns if column_name not in dataframe.columns
    ]

    if missing_columns:
        missing_column_names = ", ".join(missing_columns)
        raise ValueError(f"Input data is missing required columns: {missing_column_names}")


def require_no_rows(dataframe: DataFrame, error_message: str) -> None:
    if dataframe.limit(1).count() > 0:
        raise ValueError(error_message)


def validate_world_bank_long_csv(dataframe: DataFrame) -> None:
    require_columns(dataframe, REQUIRED_WORLD_BANK_LONG_COLUMNS)

    required_text_filters = [
        sf.trim(sf.coalesce(sf.col(column_name), sf.lit(""))) == ""
        for column_name in REQUIRED_WORLD_BANK_TEXT_COLUMNS
    ]
    missing_required_text_filter = required_text_filters[0]

    for next_filter in required_text_filters[1:]:
        missing_required_text_filter = missing_required_text_filter | next_filter

    year_text = sf.trim(sf.coalesce(sf.col(YEAR_COLUMN), sf.lit("")))
    value_text = sf.trim(sf.coalesce(sf.col(VALUE_COLUMN), sf.lit("")))
    value_as_double = sf.expr(
        f"try_cast(trim({escape_spark_identifier(VALUE_COLUMN)}) as double)",
    )

    require_no_rows(
        dataframe.filter(missing_required_text_filter),
        "World Bank data contains rows with missing country or series identifiers.",
    )
    require_no_rows(
        dataframe.filter(~year_text.rlike(YEAR_PATTERN)),
        "World Bank data contains rows with missing or invalid years.",
    )
    require_no_rows(
        dataframe.filter((value_text != "") & value_as_double.isNull()),
        "World Bank data contains non-numeric values.",
    )


def trim_text_column(column_name: str) -> Column:
    return sf.trim(sf.col(column_name)).alias(column_name)


def read_world_bank_long_csv(spark: SparkSession, input_path: PathInput) -> DataFrame:
    """Read API-generated World Bank data that is already in long format."""
    dataframe = (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(str(input_path))
    )
    validate_world_bank_long_csv(dataframe)

    selected_columns: list[Column] = []
    year_text = sf.trim(sf.col(YEAR_COLUMN))
    value_text = sf.trim(sf.coalesce(sf.col(VALUE_COLUMN), sf.lit("")))
    value_as_double = sf.expr(
        f"try_cast(trim({escape_spark_identifier(VALUE_COLUMN)}) as double)",
    )

    for column_name in OUTPUT_COLUMNS:
        if column_name == YEAR_COLUMN:
            selected_columns.append(year_text.cast(IntegerType()).alias(column_name))
        elif column_name == VALUE_COLUMN:
            selected_columns.append(
                sf.when(value_text == "", sf.lit(None))
                .otherwise(value_as_double)
                .alias(column_name),
            )
        else:
            selected_columns.append(trim_text_column(column_name))

    return dataframe.select(*selected_columns)


def read_indicator_topic_mapping(spark: SparkSession, mapping_path: PathInput) -> DataFrame:
    dataframe = (
        spark.read.option("header", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(str(mapping_path))
    )
    require_columns(dataframe, REQUIRED_TOPIC_MAPPING_COLUMNS)

    return dataframe


def write_long_csv(dataframe: DataFrame, output_path: PathInput) -> None:
    (
        dataframe.write.mode("overwrite")
        .option("header", True)
        .csv(str(output_path))
    )


def remove_existing_output(output_file: Path) -> None:
    if not output_file.exists():
        return

    if output_file.is_dir():
        shutil.rmtree(output_file)
    else:
        output_file.unlink()


def get_single_part_file(output_directory: Path) -> Path:
    part_files = sorted(output_directory.glob("part-*.csv"))

    if len(part_files) != 1:
        raise RuntimeError(
            f"Expected one CSV part file in {output_directory}, found {len(part_files)}.",
        )

    return part_files[0]


def write_single_csv(dataframe: DataFrame, output_path: PathInput) -> None:
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    temporary_output_directory = output_file.with_name(
        f".{output_file.name}.{uuid4().hex}.tmp",
    )
    staging_file = output_file.with_name(f".{output_file.name}.{uuid4().hex}.staging")

    try:
        (
            dataframe.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(str(temporary_output_directory))
        )

        part_file = get_single_part_file(temporary_output_directory)
        shutil.move(str(part_file), staging_file)

        if output_file.exists() and output_file.is_dir():
            shutil.rmtree(output_file)

        staging_file.replace(output_file)
    finally:
        # cleanup
        if staging_file.exists():
            staging_file.unlink()

        if temporary_output_directory.exists():
            shutil.rmtree(temporary_output_directory)


def slugify_topic_name(topic: str) -> str:
    """Convert a topic name into a lowercase underscore-separated filename."""
    normalized_topic = topic.lower()
    slug = NON_ALPHANUMERIC_PATTERN.sub("_", normalized_topic).strip("_")

    if not slug:
        raise ValueError("Topic name must include at least one letter or number.")

    return slug


def get_topic_output_path(output_directory: Path, topic: str) -> Path:
    topic_slug = slugify_topic_name(topic)

    return output_directory / f"{topic_slug}_long.csv"


def get_topic_indicator_column_output_path(output_directory: Path, topic: str) -> Path:
    topic_slug = slugify_topic_name(topic)

    return output_directory / f"{topic_slug}.csv"


def get_topic_wide_output_path(output_directory: Path, topic: str) -> Path:
    topic_slug = slugify_topic_name(topic)

    return output_directory / f"{topic_slug}_wide.csv"


def get_ordered_topics(dataframe: DataFrame) -> list[str]:
    return [
        row[TOPIC_COLUMN]
        for row in dataframe.select(TOPIC_COLUMN).distinct().orderBy(TOPIC_COLUMN).collect()
    ]


def remove_stale_long_topic_outputs(output_directory: Path) -> None:
    for output_file in output_directory.glob("*_long.csv"):
        remove_existing_output(output_file)


def remove_stale_indicator_column_topic_outputs(output_directory: Path) -> None:
    for output_file in output_directory.glob("*.csv"):
        if not output_file.name.endswith(("_long.csv", "_wide.csv")):
            remove_existing_output(output_file)


def remove_stale_wide_topic_outputs(output_directory: Path) -> None:
    for output_file in output_directory.glob("*_wide.csv"):
        remove_existing_output(output_file)


def get_topic_output_paths(
    output_directory: Path,
    topics: Sequence[str],
    path_getter: TopicPathGetter,
) -> dict[str, Path]:
    output_paths_by_topic: dict[str, Path] = {}
    topics_by_output_path: dict[Path, str] = {}

    for topic in topics:
        output_path = path_getter(output_directory, topic)
        existing_topic = topics_by_output_path.get(output_path)

        if existing_topic is not None:
            raise ValueError(
                f"Topics {existing_topic!r} and {topic!r} produce the same output file: "
                f"{output_path.name}",
            )

        topics_by_output_path[output_path] = topic
        output_paths_by_topic[topic] = output_path

    return output_paths_by_topic


def write_topic_csvs(dataframe: DataFrame, output_directory: PathInput) -> None:
    """Write 1 long-format CSV file for each topic in the dataframe."""
    output_directory_path = Path(output_directory)
    output_directory_path.mkdir(parents=True, exist_ok=True)
    topic_output_paths = get_topic_output_paths(
        output_directory_path,
        get_ordered_topics(dataframe),
        get_topic_output_path,
    )
    remove_stale_long_topic_outputs(output_directory_path)

    for topic, topic_output_path in topic_output_paths.items():
        topic_dataframe = dataframe.filter(sf.col(TOPIC_COLUMN) == topic).select(*OUTPUT_COLUMNS)
        write_single_csv(topic_dataframe, topic_output_path)


def write_topic_indicator_column_csvs(dataframe: DataFrame, output_directory: PathInput) -> None:
    """Write 1 indicator-column CSV file for each topic in the dataframe."""
    output_directory_path = Path(output_directory)
    output_directory_path.mkdir(parents=True, exist_ok=True)
    topic_output_paths = get_topic_output_paths(
        output_directory_path,
        get_ordered_topics(dataframe),
        get_topic_indicator_column_output_path,
    )
    remove_stale_indicator_column_topic_outputs(output_directory_path)

    for topic, topic_output_path in topic_output_paths.items():
        topic_dataframe = dataframe.filter(sf.col(TOPIC_COLUMN) == topic).select(*OUTPUT_COLUMNS)
        indicator_column_dataframe = convert_long_to_indicator_columns(topic_dataframe)
        write_single_csv(indicator_column_dataframe, topic_output_path)


def write_topic_wide_csvs(dataframe: DataFrame, output_directory: PathInput) -> None:
    """Write 1 year-column wide CSV file for each topic in the dataframe."""
    output_directory_path = Path(output_directory)
    output_directory_path.mkdir(parents=True, exist_ok=True)
    topic_output_paths = get_topic_output_paths(
        output_directory_path,
        get_ordered_topics(dataframe),
        get_topic_wide_output_path,
    )
    remove_stale_wide_topic_outputs(output_directory_path)

    for topic, topic_output_path in topic_output_paths.items():
        topic_dataframe = dataframe.filter(sf.col(TOPIC_COLUMN) == topic).select(*OUTPUT_COLUMNS)
        wide_dataframe = convert_long_to_year_columns(topic_dataframe)
        write_single_csv(wide_dataframe, topic_output_path)
