"""Tests de transform.py: forma, nombres y tipos de las filas de staging."""

from datetime import date

import pandas as pd
import pytest
from conftest import make_raw_response

from weather_etl.transform import STAGING_COLUMNS, TransformError, transform_responses


def test_parallel_lists_become_one_row_per_day():
    raw = make_raw_response(
        time=["2026-10-01", "2026-10-02"],
        temperature_2m_max=[20.0, 21.0],
        temperature_2m_min=[10.0, 11.0],
        temperature_2m_mean=[15.0, 16.0],
        precipitation_sum=[0.0, 2.5],
        wind_speed_10m_max=[5.0, 6.0],
        weather_code=[0, 61],
    )

    df = transform_responses([raw])

    assert len(df) == 2
    assert df["weather_date"].tolist() == [date(2026, 10, 1), date(2026, 10, 2)]
    assert df["precipitation_mm"].tolist() == [0.0, 2.5]


def test_output_has_staging_columns_in_order(valid_df):
    assert list(valid_df.columns) == STAGING_COLUMNS


def test_several_cities_are_concatenated(valid_df):
    assert valid_df["city_id"].tolist() == ["santiago", "lima"]


def test_column_types_match_staging_table(valid_df):
    assert valid_df["temperature_max_c"].dtype == "float64"
    assert valid_df["weather_code"].dtype == "Int64"
    assert isinstance(valid_df["weather_date"].iloc[0], date)


def test_integers_from_api_are_stored_as_float():
    # La API a veces manda 10 en vez de 10.0: el tipo no debe depender de eso.
    df = transform_responses([make_raw_response(temperature_2m_min=[10])])
    assert df["temperature_min_c"].dtype == "float64"


def test_nulls_pass_through_for_validate_to_decide():
    df = transform_responses([make_raw_response(temperature_2m_max=[None], weather_code=[None])])
    assert pd.isna(df.loc[0, "temperature_max_c"])
    assert pd.isna(df.loc[0, "weather_code"])


def test_empty_input_returns_empty_dataframe_with_types():
    df = transform_responses([])
    assert df.empty
    assert list(df.columns) == STAGING_COLUMNS
    assert df["temperature_max_c"].dtype == "float64"


def test_missing_field_raises():
    raw = make_raw_response()
    del raw["payload"]["daily"]["weather_code"]
    with pytest.raises(TransformError, match="weather_code"):
        transform_responses([raw])


def test_missing_daily_block_raises():
    raw = make_raw_response()
    raw["payload"] = {"error": True}
    with pytest.raises(TransformError, match="daily"):
        transform_responses([raw])


def test_unexpected_unit_raises():
    # Si la API cambiara a Fahrenheit, cargariamos datos falsos sin darnos cuenta.
    raw = make_raw_response()
    raw["payload"]["daily_units"]["temperature_2m_max"] = "°F"
    with pytest.raises(TransformError, match="Unidad inesperada"):
        transform_responses([raw])
