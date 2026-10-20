"""Load: lee y escribe en el warehouse PostgreSQL.

Ninguna funcion hace commit: recibe una conexion abierta y quien la llama
decide donde empieza y termina la transaccion. Asi se pueden agrupar varias
escrituras en una sola transaccion: o se guardan todas, o ninguna.
"""

import logging
import os
from datetime import date

import pandas as pd
import psycopg2
from psycopg2.extensions import connection as Connection
from psycopg2.extras import Json, execute_values

from weather_etl.transform import STAGING_COLUMNS

logger = logging.getLogger(__name__)

MEASURE_COLUMNS = [c for c in STAGING_COLUMNS if c not in ("city_id", "weather_date")]

INSERT_RAW_SQL = """
    INSERT INTO raw.weather_api_responses (city_id, logical_date, request_url, payload)
    VALUES %s
"""

# DISTINCT ON se queda con la primera fila de cada ciudad segun el ORDER BY:
# es decir, la extraccion mas reciente de ese dia.
SELECT_LATEST_RAW_SQL = """
    SELECT DISTINCT ON (city_id) city_id, logical_date, request_url, payload
    FROM raw.weather_api_responses
    WHERE logical_date = %s
    ORDER BY city_id, ingested_at DESC, id DESC
"""

UPSERT_CITIES_SQL = """
    INSERT INTO analytics.dim_city (city_id, city_name, country, latitude, longitude, timezone)
    VALUES %s
    ON CONFLICT (city_id) DO UPDATE SET
        city_name  = EXCLUDED.city_name,
        country    = EXCLUDED.country,
        latitude   = EXCLUDED.latitude,
        longitude  = EXCLUDED.longitude,
        timezone   = EXCLUDED.timezone,
        updated_at = now()
    WHERE (dim_city.city_name, dim_city.country, dim_city.latitude,
           dim_city.longitude, dim_city.timezone)
          IS DISTINCT FROM
          (EXCLUDED.city_name, EXCLUDED.country, EXCLUDED.latitude,
           EXCLUDED.longitude, EXCLUDED.timezone)
    RETURNING (xmax = 0) AS inserted
"""

# Upsert idempotente:
# - Si (city_id, weather_date) no existe -> INSERT.
# - Si existe y algun valor cambio (Open-Meteo corrige datos recientes) -> UPDATE.
# - Si existe y nada cambio -> no hace nada (updated_at no se toca).
UPSERT_DAILY_WEATHER_SQL = f"""
    INSERT INTO staging.daily_weather ({", ".join(STAGING_COLUMNS)})
    VALUES %s
    ON CONFLICT (city_id, weather_date) DO UPDATE SET
        {", ".join(f"{c} = EXCLUDED.{c}" for c in MEASURE_COLUMNS)},
        updated_at = now()
    WHERE ({", ".join(f"daily_weather.{c}" for c in MEASURE_COLUMNS)})
          IS DISTINCT FROM
          ({", ".join(f"EXCLUDED.{c}" for c in MEASURE_COLUMNS)})
    RETURNING (xmax = 0) AS inserted
"""


def connect_from_env() -> Connection:
    """Abre una conexion al warehouse usando las variables WAREHOUSE_* (para uso local).

    Dentro de Airflow no se usa: la conexion la entrega PostgresHook.
    """
    return psycopg2.connect(
        host=os.environ.get("WAREHOUSE_HOST", "localhost"),
        port=int(os.environ.get("WAREHOUSE_PORT", "5433")),
        dbname=os.environ["WAREHOUSE_DB"],
        user=os.environ["WAREHOUSE_USER"],
        password=os.environ["WAREHOUSE_PASSWORD"],
    )


def insert_raw_responses(conn: Connection, raw_responses: list[dict]) -> int:
    """Guarda las respuestas crudas de la API (append-only). Devuelve filas insertadas."""
    rows = [
        (r["city_id"], r["logical_date"], r["request_url"], Json(r["payload"]))
        for r in raw_responses
    ]
    with conn.cursor() as cur:
        execute_values(cur, INSERT_RAW_SQL, rows)
    logger.info("raw.weather_api_responses: %d filas insertadas", len(rows))
    return len(rows)


def fetch_latest_raw_responses(conn: Connection, logical_date: date) -> list[dict]:
    """Lee de raw la ultima respuesta de cada ciudad para un dia.

    Devuelve dicts con la misma forma que extract.fetch_daily_weather, asi
    transform.py funciona igual leyendo de la API o de la base.
    """
    with conn.cursor() as cur:
        cur.execute(SELECT_LATEST_RAW_SQL, (logical_date,))
        columns = [col.name for col in cur.description]
        responses = [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]
    logger.info("raw: %d respuestas leidas para %s", len(responses), logical_date)
    return responses


def upsert_cities(conn: Connection, cities: list[dict]) -> dict[str, int]:
    """Sincroniza analytics.dim_city con la configuracion de ciudades."""
    rows = [
        (c["city_id"], c["city_name"], c["country"], c["latitude"], c["longitude"], c["timezone"])
        for c in cities
    ]
    return _run_upsert(conn, UPSERT_CITIES_SQL, rows, "analytics.dim_city")


def upsert_daily_weather(conn: Connection, df: pd.DataFrame) -> dict[str, int]:
    """Carga el DataFrame validado en staging.daily_weather (upsert idempotente)."""
    return _run_upsert(
        conn, UPSERT_DAILY_WEATHER_SQL, _dataframe_to_rows(df), "staging.daily_weather"
    )


def _run_upsert(conn: Connection, sql: str, rows: list[tuple], table: str) -> dict[str, int]:
    """Ejecuta un upsert y cuenta cuantas filas se insertaron, actualizaron o quedaron igual."""
    if not rows:
        logger.warning("%s: no hay filas para cargar", table)
        return {"inserted": 0, "updated": 0, "unchanged": 0}

    with conn.cursor() as cur:
        # fetch=True devuelve lo que dice RETURNING: un booleano por fila tocada.
        # xmax = 0 es un truco de PostgreSQL: es verdadero solo en filas recien insertadas.
        results = execute_values(cur, sql, rows, fetch=True)

    inserted = sum(1 for (was_inserted,) in results if was_inserted)
    updated = len(results) - inserted
    stats = {"inserted": inserted, "updated": updated, "unchanged": len(rows) - len(results)}
    logger.info("%s: %s", table, stats)
    return stats


def _dataframe_to_rows(df: pd.DataFrame) -> list[tuple]:
    """Convierte el DataFrame en tuplas de tipos Python que psycopg2 sabe enviar.

    pandas usa NaN/<NA> para los nulos y tipos de numpy para los numeros;
    PostgreSQL necesita None y int/float de Python.
    """
    return [
        tuple(_to_python(value) for value in row)
        for row in df[STAGING_COLUMNS].itertuples(index=False, name=None)
    ]


def _to_python(value):
    if pd.isna(value):
        return None
    if hasattr(value, "item"):  # numpy.int64 -> int, numpy.float64 -> float
        return value.item()
    return value


if __name__ == "__main__":
    # Prueba manual (necesita el warehouse corriendo en Docker):
    #   uv run --env-file .env python -m weather_etl.load
    from datetime import timedelta

    from weather_etl.extract import fetch_daily_weather, load_cities
    from weather_etl.transform import transform_responses
    from weather_etl.validate import validate_daily_weather

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    cities = load_cities()
    yesterday = date.today() - timedelta(days=1)

    conn = connect_from_env()
    try:
        logger.info("=== 1) Extract -> raw ===")
        raw_responses = [fetch_daily_weather(city, yesterday) for city in cities]
        with conn:  # commit al salir del bloque; rollback si hubo una excepcion
            insert_raw_responses(conn, raw_responses)

        logger.info("=== 2) raw -> transform -> validate -> staging ===")
        df = transform_responses(fetch_latest_raw_responses(conn, yesterday))
        validate_daily_weather(
            df, expected_city_ids=[c["city_id"] for c in cities], expected_date=yesterday
        )
        with conn:  # dim_city y staging en la misma transaccion: todo o nada
            upsert_cities(conn, cities)
            upsert_daily_weather(conn, df)

        logger.info("=== 3) Misma carga otra vez: no debe duplicar nada ===")
        with conn:
            upsert_daily_weather(conn, df)

        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM staging.daily_weather WHERE weather_date = %s", (yesterday,)
            )
            logger.info("Filas en staging para %s: %d", yesterday, cur.fetchone()[0])
    finally:
        conn.close()
