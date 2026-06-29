from pathlib import Path

import pytest

from pyspark.sql import SparkSession

from world_bank_pipeline import pipeline
from world_bank_pipeline.pipeline import (
    DEFAULT_SPARK_SQL_SHUFFLE_PARTITIONS,
    SPARK_SQL_SHUFFLE_PARTITIONS_ENV_VAR,
    PipelinePaths,
    get_spark_sql_shuffle_partitions,
    run_pipeline,
)


def read_parquet_rows(
    spark: SparkSession,
    output_directory: Path,
) -> list[dict[str, object]]:
    return [row.asDict() for row in spark.read.parquet(str(output_directory)).collect()]


def read_csv_rows(
    spark: SparkSession,
    output_file: Path,
) -> list[dict[str, object]]:
    return [
        row.asDict()
        for row in spark.read.option("header", True).csv(str(output_file)).collect()
    ]


def test_main_runs_pipeline_with_supplied_paths(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    paths = PipelinePaths(
        api_indicator_output_path=tmp_path / "world_bank_api_indicator_data.parquet",
        mapping_path=tmp_path / "indicator_topic_mapping.csv",
        long_output_path=tmp_path / "world_bank_indicators_long.parquet",
        indicator_wide_output_path=tmp_path / "world_bank_indicators_indicator_wide.parquet",
        year_wide_output_path=tmp_path / "world_bank_indicators_year_wide.parquet",
        mapping_output_path=tmp_path / "filtered_indicator_topic_mapping.csv",
    )
    calls: list[PipelinePaths] = []

    def fake_run_pipeline(run_paths: PipelinePaths) -> None:
        calls.append(run_paths)

    monkeypatch.setattr(pipeline, "run_pipeline", fake_run_pipeline)

    pipeline.main(paths)

    assert calls == [paths]


def test_get_spark_sql_shuffle_partitions_uses_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(SPARK_SQL_SHUFFLE_PARTITIONS_ENV_VAR, raising=False)

    assert get_spark_sql_shuffle_partitions() == DEFAULT_SPARK_SQL_SHUFFLE_PARTITIONS


def test_get_spark_sql_shuffle_partitions_uses_environment_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(SPARK_SQL_SHUFFLE_PARTITIONS_ENV_VAR, "12")

    assert get_spark_sql_shuffle_partitions() == 12


@pytest.mark.parametrize("raw_partition_count", ["0", "-1", "not-a-number"])
def test_get_spark_sql_shuffle_partitions_rejects_invalid_values(
    monkeypatch: pytest.MonkeyPatch,
    raw_partition_count: str,
) -> None:
    monkeypatch.setenv(SPARK_SQL_SHUFFLE_PARTITIONS_ENV_VAR, raw_partition_count)

    with pytest.raises(ValueError, match="must be a positive integer"):
        get_spark_sql_shuffle_partitions()


def test_run_pipeline_writes_filtered_outputs(tmp_path: Path) -> None:
    """Integration test for end-to-end pipeline execution."""

    api_input = tmp_path / "world_bank_api_indicator_data.parquet"
    topic_mapping = tmp_path / "indicator_topic_mapping.csv"
    paths = PipelinePaths(
        api_indicator_output_path=api_input,
        mapping_path=topic_mapping,
        long_output_path=tmp_path / "world_bank_indicators_long.parquet",
        indicator_wide_output_path=tmp_path / "world_bank_indicators_indicator_wide.parquet",
        year_wide_output_path=tmp_path / "world_bank_indicators_year_wide.parquet",
        mapping_output_path=tmp_path / "indicator_topic_mapping.csv",
    )
    spark = pipeline.create_spark_session()

    try:
        spark.createDataFrame(
            [
                ("SP.ADO.TFRT", "Health indicator", "ARG", "Argentina", 2023, 1.5),
                ("SP.ADO.TFRT", "Health indicator", "WLD", "World", 2023, 2.5),
                ("SP.ADO.TFRT", "Health indicator", "", "High income", 2023, 3.5),
                ("SP.ADO.TFRT", "Health indicator", "BRA", "Brazil", 2023, None),
                ("NV.AGR.TOTL.ZS", "Agriculture indicator", "ZWE", "Zimbabwe", 2024, 3.0),
            ],
            [
                "Series Code",
                "Series Name",
                "Country Code",
                "Country Name",
                "Year",
                "Value",
            ],
        ).write.parquet(str(api_input))
    finally:
        spark.stop()

    topic_mapping.write_text(
        "id,name,source_id,source,source_organization,topic\n"
        "SP.ADO.TFRT,Health indicator,2,World Development Indicators,World Bank,Health\n"
        "NV.AGR.TOTL.ZS,Agriculture indicator,2,World Development Indicators,World Bank,Agriculture & Rural Development\n"
        "UNUSED,Unused indicator,2,World Development Indicators,World Bank,Unused\n",
        encoding="utf-8",
    )

    run_pipeline(paths)
    spark = pipeline.create_spark_session()

    try:
        long_rows = read_parquet_rows(spark, paths.long_output_path)
        indicator_wide_rows = read_parquet_rows(spark, paths.indicator_wide_output_path)
        year_wide_rows = read_parquet_rows(spark, paths.year_wide_output_path)
        mapping_rows = read_csv_rows(spark, paths.mapping_output_path)
    finally:
        spark.stop()

    assert list(paths.long_output_path.glob("part-*.parquet"))
    assert list(paths.indicator_wide_output_path.glob("part-*.parquet"))
    assert list(paths.year_wide_output_path.glob("part-*.parquet"))
    assert paths.mapping_output_path.exists()
    assert sorted(long_rows, key=lambda row: str(row["Country Code"])) == [
        {
            "Country Name": "Argentina",
            "Country Code": "ARG",
            "Series Name": "Health indicator",
            "Series Code": "SP.ADO.TFRT",
            "Year": 2023,
            "Value": 1.5,
            "Topic": "Health",
        },
        {
            "Country Name": "Zimbabwe",
            "Country Code": "ZWE",
            "Series Name": "Agriculture indicator",
            "Series Code": "NV.AGR.TOTL.ZS",
            "Year": 2024,
            "Value": 3.0,
            "Topic": "Agriculture & Rural Development",
        },
    ]
    assert sorted(
        indicator_wide_rows,
        key=lambda row: str(row["Country Code"]),
    ) == [
        {
            "Country Name": "Argentina",
            "Country Code": "ARG",
            "Year": 2023,
            "Topic": "Health",
            "Agriculture indicator": None,
            "Health indicator": 1.5,
        },
        {
            "Country Name": "Zimbabwe",
            "Country Code": "ZWE",
            "Year": 2024,
            "Topic": "Agriculture & Rural Development",
            "Agriculture indicator": 3.0,
            "Health indicator": None,
        },
    ]
    assert sorted(
        year_wide_rows,
        key=lambda row: str(row["Country Code"]),
    ) == [
        {
            "Country Name": "Argentina",
            "Country Code": "ARG",
            "Series Name": "Health indicator",
            "Series Code": "SP.ADO.TFRT",
            "Topic": "Health",
            "2023": 1.5,
            "2024": None,
        },
        {
            "Country Name": "Zimbabwe",
            "Country Code": "ZWE",
            "Series Name": "Agriculture indicator",
            "Series Code": "NV.AGR.TOTL.ZS",
            "Topic": "Agriculture & Rural Development",
            "2023": None,
            "2024": 3.0,
        },
    ]
    assert sorted(mapping_rows, key=lambda row: str(row["id"])) == [
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
