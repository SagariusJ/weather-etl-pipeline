"""Tests de load.py.

Los primeros son unitarios (sin base de datos). Los marcados como
"integration" corren contra un PostgreSQL real: en tu PC, la base weather_test
de Docker; en GitHub Actions, un PostgreSQL temporal que se crea para el CI.
"""

from datetime import date

import numpy as np
import pandas as pd
import pytest
from conftest import make_raw_response

from weather_etl.load import (
    _dataframe_to_rows,
    _to_python,
    fetch_latest_raw_responses,
    insert_raw_responses,
    upsert_cities,
    upsert_daily_weather,
)
from weather_etl.transform import transform_responses

CITIES = [
    {
        "city_id": "santiago",
        "city_name": "Santiago",
        "country": "CL",
        "latitude": -33.4489,
        "longitude": -70.6693,
        "timezone": "America/Santiago",
    },
    {
        "city_id": "lima",
        "city_name": "Lima",
        "country": "PE",
        "latitude": -12.0464,
        "longitude": -77.0428,
        "timezone": "America/Lima",
    },
]


# --- Unitarios -------------------------------------------------------------------


@pytest.mark.parametrize("missing", [None, np.nan, pd.NA])
def test_pandas_nulls_become_none(missing):
    assert _to_python(missing) is None


def test_numpy_numbers_become_python_numbers():
    assert type(_to_python(np.float64(1.5))) is float
    assert type(_to_python(np.int64(3))) is int


def test_dataframe_rows_are_plain_python(valid_df):
    rows = _dataframe_to_rows(valid_df)
    assert len(rows) == 2
    assert rows[0][:2] == ("santiago", date(2026, 10, 1))
    assert all(type(v) in (str, date, float, int) for v in rows[0])


# --- Integracion (PostgreSQL real) -----------------------------------------------


def staging_count(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM staging.daily_weather")
        return cur.fetchone()[0]


@pytest.mark.integration
def test_upsert_is_idempotent(db_conn, valid_df):
    with db_conn:
        first = upsert_daily_weather(db_conn, valid_df)
    with db_conn:
        second = upsert_daily_weather(db_conn, valid_df)

    assert first == {"inserted": 2, "updated": 0, "unchanged": 0}
    assert second == {"inserted": 0, "updated": 0, "unchanged": 2}
    assert staging_count(db_conn) == 2  # cargar dos veces no duplica


@pytest.mark.integration
def test_upsert_updates_only_changed_rows(db_conn, valid_df):
    with db_conn:
        upsert_daily_weather(db_conn, valid_df)

    corrected = valid_df.copy()
    corrected.loc[corrected["city_id"] == "lima", "temperature_max_c"] = 25.0
    with db_conn:
        stats = upsert_daily_weather(db_conn, corrected)

    assert stats == {"inserted": 0, "updated": 1, "unchanged": 1}
    with db_conn.cursor() as cur:
        cur.execute("SELECT temperature_max_c FROM staging.daily_weather WHERE city_id = 'lima'")
        assert float(cur.fetchone()[0]) == 25.0


@pytest.mark.integration
def test_nulls_are_stored_as_sql_null(db_conn):
    df = transform_responses([make_raw_response(wind_speed_10m_max=[None], weather_code=[None])])
    with db_conn:
        upsert_daily_weather(db_conn, df)

    with db_conn.cursor() as cur:
        cur.execute("SELECT wind_speed_max_kmh, weather_code FROM staging.daily_weather")
        assert cur.fetchone() == (None, None)


@pytest.mark.integration
def test_raw_roundtrip_keeps_only_latest_response_per_city(db_conn):
    old = make_raw_response("santiago", temperature_2m_max=[18.0])
    new = make_raw_response("santiago", temperature_2m_max=[19.0])
    with db_conn:
        insert_raw_responses(db_conn, [old])
    with db_conn:
        insert_raw_responses(db_conn, [new])

    responses = fetch_latest_raw_responses(db_conn, date(2026, 10, 1))

    assert len(responses) == 1
    assert responses[0]["payload"]["daily"]["temperature_2m_max"] == [19.0]
    # Lo leido de raw sirve directo como entrada de transform.
    assert len(transform_responses(responses)) == 1


@pytest.mark.integration
def test_failed_transaction_leaves_nothing_behind(db_conn, valid_df):
    with pytest.raises(RuntimeError):
        with db_conn:
            upsert_cities(db_conn, CITIES)
            upsert_daily_weather(db_conn, valid_df)
            raise RuntimeError("falla simulada antes del commit")

    assert staging_count(db_conn) == 0
