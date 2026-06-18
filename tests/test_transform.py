import pytest

from src.world_bank_pipeline.transform import (
    get_year_columns,
    validate_input_columns,
)


def test_get_year_columns_returns_sorted_world_bank_years() -> None:
    columns = [
        "Country Name",
        "2025 [YR2025]",
        "notes",
        "2024 [YR2024]",
    ]

    assert get_year_columns(columns) == [
        ("2024 [YR2024]", 2024),
        ("2025 [YR2025]", 2025),
    ]


def test_validate_input_columns_requires_identifiers_and_years() -> None:
    with pytest.raises(ValueError, match="Country Code"):
        validate_input_columns(["Country Name", "2024 [YR2024]"], [("2024 [YR2024]", 2024)])

    with pytest.raises(ValueError, match="does not include any columns"):
        validate_input_columns(
            ["Country Name", "Country Code", "Series Name", "Series Code"],
            [],
        )
