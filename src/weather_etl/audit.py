"""Audit: deja un registro de cada ejecucion del pipeline en audit.pipeline_runs.

Cada corrida del DAG inserta una fila al empezar (status 'running') y la cierra
al terminar ('success' o 'failed'). Asi se puede responder con SQL preguntas como
"¿que dias fallaron este mes?" o "¿cuantas filas cargamos ayer?".

Igual que en load.py, ninguna funcion hace commit: lo decide quien llama.
"""

import logging
from datetime import date

from psycopg2.extensions import connection as Connection

logger = logging.getLogger(__name__)

# Solo se actualiza la fila que sigue en 'running': si el DAG se re-ejecuta,
# las corridas anteriores (ya cerradas) quedan intactas como historial.
_OPEN_RUN = "dag_id = %s AND run_id = %s AND status = 'running'"


def start_run(conn: Connection, dag_id: str, run_id: str, logical_date: date) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO audit.pipeline_runs (dag_id, run_id, logical_date, status)
            VALUES (%s, %s, %s, 'running')
            """,
            (dag_id, run_id, logical_date),
        )
    logger.info("Auditoria: inicio de %s para %s", run_id, logical_date)


def record_extracted(conn: Connection, dag_id: str, run_id: str, rows_extracted: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE audit.pipeline_runs SET rows_extracted = %s WHERE {_OPEN_RUN}",
            (rows_extracted, dag_id, run_id),
        )


def finish_run(conn: Connection, dag_id: str, run_id: str, rows_loaded: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE audit.pipeline_runs
            SET status = 'success', rows_loaded = %s, finished_at = now()
            WHERE {_OPEN_RUN}
            """,
            (rows_loaded, dag_id, run_id),
        )
    logger.info("Auditoria: %s termino OK (%d filas cargadas)", run_id, rows_loaded)


def fail_run(conn: Connection, dag_id: str, run_id: str, error_message: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE audit.pipeline_runs
            SET status = 'failed', error_message = %s, finished_at = now()
            WHERE {_OPEN_RUN}
            """,
            (error_message[:2000], dag_id, run_id),
        )
    logger.error("Auditoria: %s marcado como fallido", run_id)
