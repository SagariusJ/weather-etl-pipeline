"""DAG principal: extract -> transform -> validate -> load, una vez al dia.

    start_run -> extract_to_raw -> transform_validate_load -> finish_run

Cada corrida procesa el DIA ANTERIOR a su fecha logica: la corrida del 15/10
a las 09:00 UTC carga el clima del 14/10 (ya terminado en todas las ciudades).

Backfill (cargar dias pasados). Ejemplo para los dias 1 al 7 de octubre: son las
corridas del 2 al 8, porque cada una carga el dia anterior. Dentro del scheduler
(docker compose exec airflow-scheduler bash):
  airflow backfill create --dag-id weather_etl --from-date 2026-10-02 --to-date 2026-10-08T09:00
Los dias que ya tienen corrida se saltan; para rehacerlos: --reprocess-behavior completed
"""

import logging
from contextlib import closing
from datetime import date, timedelta

import pendulum
import psycopg2
from airflow.sdk import BaseHook, dag, task

from weather_etl import audit
from weather_etl.extract import fetch_daily_weather, load_cities
from weather_etl.load import (
    fetch_latest_raw_responses,
    insert_raw_responses,
    upsert_cities,
    upsert_daily_weather,
)
from weather_etl.transform import transform_responses
from weather_etl.validate import validate_daily_weather

logger = logging.getLogger(__name__)

WAREHOUSE_CONN_ID = "warehouse_db"


def warehouse_connection():
    """Conexion psycopg2 al warehouse, con las credenciales de la conexion de Airflow.

    No usamos PostgresHook.get_conn() porque en esta version devuelve una conexion
    de psycopg 3, y load.py esta escrito para psycopg2.
    """
    c = BaseHook.get_connection(WAREHOUSE_CONN_ID)
    conn = psycopg2.connect(
        host=c.host, port=c.port, dbname=c.schema, user=c.login, password=c.password
    )
    return closing(conn)  # closing(): la conexion se cierra al salir del "with"


def target_day(context) -> date:
    """Dia a procesar: el anterior a la fecha logica de la corrida."""
    # En corridas manuales de Airflow 3 logical_date puede venir vacio; usamos run_after.
    reference = context.get("logical_date") or context["dag_run"].run_after
    return (reference - timedelta(days=1)).date()


def mark_run_failed(context) -> None:
    """Callback: cuando una tarea agota sus reintentos y falla, cierra la auditoria."""
    ti = context["ti"]
    error = f"{ti.task_id}: {context.get('exception')}"
    try:
        with warehouse_connection() as conn, conn:
            audit.fail_run(conn, ti.dag_id, context["run_id"], error)
    except Exception:
        # Un error en el callback no debe tapar el error original de la tarea.
        logger.exception("No se pudo registrar la falla en la auditoria")


@dag(
    dag_id="weather_etl",
    schedule="0 9 * * *",  # todos los dias a las 09:00 UTC
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,  # al activarlo no corre todos los dias pasados; para eso esta el backfill
    max_active_runs=3,  # en un backfill, maximo 3 dias en paralelo (cuidar la API)
    dagrun_timeout=timedelta(hours=1),
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,  # 5 min, luego 10 min
        "on_failure_callback": mark_run_failed,
    },
    tags=["weather", "etl"],
)
def weather_etl():
    @task
    def start_run(**context) -> None:
        with warehouse_connection() as conn, conn:
            audit.start_run(conn, context["dag"].dag_id, context["run_id"], target_day(context))

    @task
    def extract_to_raw(**context) -> int:
        day = target_day(context)
        raw_responses = [fetch_daily_weather(city, day) for city in load_cities()]
        # Commit propio: la respuesta cruda queda guardada aunque despues falle la validacion.
        with warehouse_connection() as conn, conn:
            insert_raw_responses(conn, raw_responses)
            audit.record_extracted(
                conn, context["dag"].dag_id, context["run_id"], len(raw_responses)
            )
        return len(raw_responses)

    @task
    def transform_validate_load(**context) -> dict:
        day = target_day(context)
        cities = load_cities()
        with warehouse_connection() as conn:
            df = transform_responses(fetch_latest_raw_responses(conn, day))
            validate_daily_weather(
                df, expected_city_ids=[c["city_id"] for c in cities], expected_date=day
            )
            with conn:  # dim_city y staging en una sola transaccion: todo o nada
                upsert_cities(conn, cities)
                stats = upsert_daily_weather(conn, df)
        return stats

    @task
    def finish_run(load_stats: dict, **context) -> None:
        rows_loaded = load_stats["inserted"] + load_stats["updated"]
        with warehouse_connection() as conn, conn:
            audit.finish_run(conn, context["dag"].dag_id, context["run_id"], rows_loaded)

    extracted = extract_to_raw()
    load_stats = transform_validate_load()
    start_run() >> extracted >> load_stats
    finish_run(load_stats)


weather_etl()
