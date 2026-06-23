import csv
from pathlib import Path

import pytest

from world_bank_pipeline import pipeline
from world_bank_pipeline.pipeline import PipelinePaths, run_pipeline


def read_csv_rows(output_file: Path) -> list[dict[str, str]]:
    with output_file.open(newline="", encoding="utf-8") as csv_file:
        return list(csv.DictReader(csv_file))


def test_main_runs_pipeline_with_supplied_paths(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    paths = PipelinePaths(
        api_indicator_output_path=tmp_path / "world_bank_api_indicator_data.csv",
        mapping_path=tmp_path / "indicator_topic_mapping.csv",
        partitioned_output_path=tmp_path / "partitioned",
        single_output_path=tmp_path / "world_bank_indicators_long.csv",
        topic_output_path=tmp_path / "topics",
    )
    calls: list[PipelinePaths] = []

    def fake_run_pipeline(run_paths: PipelinePaths) -> None:
        calls.append(run_paths)

    monkeypatch.setattr(pipeline, "run_pipeline", fake_run_pipeline)

    pipeline.main(paths)

    assert calls == [paths]


def test_run_pipeline_writes_filtered_outputs(tmp_path: Path) -> None:
    """Integration test for end-to-end pipeline execution."""
    
    api_input = tmp_path / "world_bank_api_indicator_data.csv"
    topic_mapping = tmp_path / "indicator_topic_mapping.csv"
    paths = PipelinePaths(
        api_indicator_output_path=api_input,
        mapping_path=topic_mapping,
        partitioned_output_path=tmp_path / "partitioned",
        single_output_path=tmp_path / "world_bank_indicators_long.csv",
        topic_output_path=tmp_path / "topics",
    )
    api_input.write_text(
        "Series Code,Series Name,Country Code,Country Name,Year,Value\n"
        "SP.ADO.TFRT,Health indicator,ARG,Argentina,2023,1.5\n"
        "SP.ADO.TFRT,Health indicator,WLD,World,2023,2.5\n"
        "SP.ADO.TFRT,Health indicator,BRA,Brazil,2023,\n"
        "NV.AGR.TOTL.ZS,Agriculture indicator,ZWE,Zimbabwe,2024,3.0\n",
        encoding="utf-8",
    )
    topic_mapping.write_text(
        "id,topic\n"
        "SP.ADO.TFRT,Health\n"
        "NV.AGR.TOTL.ZS,Agriculture & Rural Development\n",
        encoding="utf-8",
    )

    run_pipeline(paths)

    assert paths.partitioned_output_path.exists()
    assert sorted(read_csv_rows(paths.single_output_path), key=lambda row: row["Country Code"]) == [
        {
            "Country Name": "Argentina",
            "Country Code": "ARG",
            "Series Name": "Health indicator",
            "Series Code": "SP.ADO.TFRT",
            "Year": "2023",
            "Value": "1.5",
        },
        {
            "Country Name": "Zimbabwe",
            "Country Code": "ZWE",
            "Series Name": "Agriculture indicator",
            "Series Code": "NV.AGR.TOTL.ZS",
            "Year": "2024",
            "Value": "3.0",
        },
    ]
    assert read_csv_rows(paths.topic_output_path / "health_long.csv") == [
        {
            "Country Name": "Argentina",
            "Country Code": "ARG",
            "Series Name": "Health indicator",
            "Series Code": "SP.ADO.TFRT",
            "Year": "2023",
            "Value": "1.5",
        },
    ]
