"""Fixtures compartidas por todos los tests.

Una fixture es algo que pytest prepara antes de un test y le entrega como
argumento: datos de ejemplo, una conexion a la base, etc.
"""

import os
from datetime import date
from pathlib import Path

import psycopg2
import pytest

from weather_etl.transform import transform_responses

SCHEMA_SQL = Path(__file__).parents[1] / "sql" / "init" / "01_schemas.sql"

DAILY_UNITS = {
    "time": "iso8601",
    "temperature_2m_max": "°C",
    "temperature_2m_min": "°C",
    "temperature_2m_mean": "°C",
    "precipitation_sum": "mm",
    "wind_speed_10m_max": "km/h",
    "weather_code": "wmo code",
}


def make_raw_response(city_id: str = "santiago", day: date = date(2026, 10, 1), **daily) -> dict:
    """Arma una respuesta falsa con la misma forma que devuelve extract.fetch_daily_weather.

    Por defecto es un dia valido; los kwargs reemplazan listas de 'daily', por ejemplo:
    make_raw_response(temperature_2m_max=[None]).
    """
    values = {
        "time": [day.isoformat()],
        "temperature_2m_max": [20.5],
        "temperature_2m_min": [10.0],
        "temperature_2m_mean": [15.2],
        "precipitation_sum": [1.5],
        "wind_speed_10m_max": [12.3],
        "weather_code": [61],
    }
    values.update(daily)
    return {
        "city_id": city_id,
        "logical_date": day,
        "request_url": f"https://example.test/{city_id}/{day}",
        "payload": {"daily": values, "daily_units": dict(DAILY_UNITS)},
    }


@pytest.fixture
def valid_df():
    """DataFrame valido de 2 ciudades, tal como sale de transform.py."""
    return transform_responses([make_raw_response("santiago"), make_raw_response("lima")])


# --- Tests de integracion (necesitan PostgreSQL) ----------------------------------


@pytest.fixture(scope="session")
def _test_database():
    """Conexion a la base de TEST. Si TEST_WAREHOUSE_DB no esta definida, se saltan."""
    dbname = os.environ.get("TEST_WAREHOUSE_DB")
    if not dbname:
        pytest.skip("TEST_WAREHOUSE_DB no definida: se omiten los tests de integracion")
    # Proteccion: los tests BORRAN tablas. Nunca deben apuntar a la base real.
    if not dbname.endswith("_test"):
        pytest.fail(f"TEST_WAREHOUSE_DB debe terminar en '_test' (es {dbname!r})")

    conn = psycopg2.connect(
        host=os.environ.get("WAREHOUSE_HOST", "localhost"),
        port=int(os.environ.get("WAREHOUSE_PORT", "15432")),
        dbname=dbname,
        user=os.environ["WAREHOUSE_USER"],
        password=os.environ["WAREHOUSE_PASSWORD"],
    )
    with conn:  # crea los esquemas y tablas con el mismo SQL que usa Docker
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL.read_text(encoding="utf-8"))
    yield conn
    conn.close()


@pytest.fixture
def db_conn(_test_database):
    """Conexion con las tablas vacias: cada test parte de cero."""
    with _test_database, _test_database.cursor() as cur:
        cur.execute(
            "TRUNCATE raw.weather_api_responses, staging.daily_weather, "
            "analytics.dim_city, audit.pipeline_runs RESTART IDENTITY"
        )
    return _test_database