from pathlib import Path

SPARK_APP_NAME = "world-bank-data-pipeline"
SPARK_SQL_SHUFFLE_PARTITIONS_ENV_VAR = "SPARK_SQL_SHUFFLE_PARTITIONS"
DEFAULT_SPARK_SQL_SHUFFLE_PARTITIONS = 8

API_INDICATOR_OUTPUT_PATH = Path("/data/world_bank_api_indicator_data.parquet")
MAPPING_PATH = Path("/mapping/indicator_topic_mapping.csv")
LONG_OUTPUT_PATH = Path("/output/world_bank_indicators_long.parquet")
INDICATOR_WIDE_OUTPUT_PATH = Path("/output/world_bank_indicators_indicator_wide.parquet")
YEAR_WIDE_OUTPUT_PATH = Path("/output/world_bank_indicators_year_wide.parquet")
MAPPING_OUTPUT_PATH = Path("/output/indicator_topic_mapping.csv")

WORLD_BANK_COUNTRY_CODES_EXCLUDE: tuple[str, ...] = (
    "AFE",
    "AFW",
    "ARB",
    "CEB",
    "CSS",
    "EAP",
    "EAR",
    "EAS",
    "ECA",
    "ECS",
    "EMU",
    "EUU",
    "FCS",
    "HIC",
    "HPC",
    "IBD",
    "IBT",
    "IDA",
    "IDB",
    "IDX",
    "INX",
    "LAC",
    "LCN",
    "LDC",
    "LIC",
    "LMC",
    "LMY",
    "LTE",
    "MEA",
    "MIC",
    "MNA",
    "NAC",
    "OED",
    "OSS",
    "PRE",
    "PSS",
    "PST",
    "SAS",
    "SSA",
    "SSF",
    "SST",
    "TEA",
    "TEC",
    "TLA",
    "TMN",
    "TSA",
    "TSS",
    "UMC",
    "WLD",
)

COUNTRY_NAME_COLUMN = "Country Name"
COUNTRY_CODE_COLUMN = "Country Code"
SERIES_NAME_COLUMN = "Series Name"
SERIES_CODE_COLUMN = "Series Code"
VALUE_COLUMN = "Value"
YEAR_COLUMN = "Year"
TOPIC_COLUMN = "Topic"

INDICATOR_COLUMN_ROW_COLUMNS = [
    COUNTRY_NAME_COLUMN,
    COUNTRY_CODE_COLUMN,
    YEAR_COLUMN,
]
YEAR_COLUMN_ROW_COLUMNS = [
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
TOPIC_OUTPUT_COLUMNS = [
    *OUTPUT_COLUMNS,
    TOPIC_COLUMN,
]

MAPPING_TOPIC_COUNT_COLUMN = "_mapping_topic_count"

YEAR_PATTERN = r"^\d{4}$"
REQUIRED_WORLD_BANK_LONG_COLUMNS = OUTPUT_COLUMNS
TOPIC_MAPPING_COLUMNS = [
    "id",
    "name",
    "source_id",
    "source",
    "source_organization",
    "topic",
]
REQUIRED_TOPIC_MAPPING_COLUMNS = ["id", "topic"]
REQUIRED_WORLD_BANK_TEXT_COLUMNS = [
    COUNTRY_NAME_COLUMN,
    SERIES_NAME_COLUMN,
    SERIES_CODE_COLUMN,
]
