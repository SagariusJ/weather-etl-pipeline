"""Tests de validate.py: cada regla de calidad detecta su problema."""

import logging
from datetime import date

import pandas as pd
import pytest

from weather_etl.validate import DataQualityError, validate_daily_weather


def test_valid_data_passes(valid_df):
    validate_daily_weather(
        valid_df, expected_city_ids=["santiago", "lima"], expected_date=date(2026, 10, 1)
    )


# Cada caso toma datos validos, rompe UNA cosa y verifica que se detecte.
# (columna, valor que la rompe, texto que debe aparecer en el error)
BROKEN_VALUES = [
    ("temperature_max_c", None, "temperature_max_c tiene 1 nulo"),
    ("precipitation_mm", None, "precipitation_mm tiene 1 nulo"),
    ("temperature_min_c", 99.0, "fuera del rango"),
    ("precipitation_mm", -1.0, "fuera del rango"),
    ("wind_speed_max_kmh", 500.0, "fuera del rango"),
    ("temperature_min_c", 25.0, "minima mayor que la maxima"),
    ("temperature_mean_c", 30.0, "media fuera de"),
    ("weather_code", 7, "weather_code no valido"),
]


@pytest.mark.parametrize(("column", "bad_value", "expected_error"), BROKEN_VALUES)
def test_broken_value_is_detected(valid_df, column, bad_value, expected_error):
    valid_df.loc[0, column] = bad_value
    with pytest.raises(DataQualityError, match=expected_error):
        validate_daily_weather(valid_df)


def test_duplicated_key_is_detected(valid_df):
    duplicated = pd.concat([valid_df, valid_df.head(1)], ignore_index=True)
    with pytest.raises(DataQualityError, match="duplicadas"):
        validate_daily_weather(duplicated)


def test_empty_dataframe_is_an_error(valid_df):
    with pytest.raises(DataQualityError, match="vacio"):
        validate_daily_weather(valid_df.iloc[0:0])


def test_missing_city_is_detected(valid_df):
    with pytest.raises(DataQualityError, match=r"Faltan ciudades: \['quito'\]"):
        validate_daily_weather(valid_df, expected_city_ids=["santiago", "lima", "quito"])


def test_wrong_date_is_detected(valid_df):
    with pytest.raises(DataQualityError, match="fecha distinta"):
        validate_daily_weather(valid_df, expected_date=date(2026, 10, 2))


def test_schema_error_stops_before_other_checks(valid_df):
    with pytest.raises(DataQualityError) as exc_info:
        validate_daily_weather(valid_df.drop(columns=["weather_code"]))
    assert exc_info.value.errors == ["Faltan columnas: ['weather_code']"]


def test_all_errors_are_reported_together(valid_df):
    valid_df.loc[0, "precipitation_mm"] = None
    valid_df.loc[1, "weather_code"] = 7
    with pytest.raises(DataQualityError) as exc_info:
        validate_daily_weather(valid_df)
    assert len(exc_info.value.errors) == 2


def test_optional_null_is_only_a_warning(valid_df, caplog):
    valid_df.loc[0, "wind_speed_max_kmh"] = None
    with caplog.at_level(logging.WARNING):
        validate_daily_weather(valid_df)  # no lanza excepcion
    assert "wind_speed_max_kmh tiene 1 nulo" in caplog.text