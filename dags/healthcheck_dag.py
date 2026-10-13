"""DAG de infraestructura: verifica que Airflow puede hablar con el warehouse
y que el paquete del proyecto (src/weather_etl) es importable.

Correrlo manualmente despues de `docker compose up` para validar el stack.
"""

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

EXPECTED_SCHEMAS = {"raw", "staging", "analytics", "audit"}


@dag(schedule=None, catchup=False, tags=["infra"])
def warehouse_healthcheck():
    @task
    def check_warehouse_schemas() -> list[str]:
        hook = PostgresHook(postgres_conn_id="warehouse_db")
        rows = hook.get_records("SELECT schema_name FROM information_schema.schemata")
        found = {row[0] for row in rows}
        missing = EXPECTED_SCHEMAS - found
        if missing:
            raise ValueError(f"Faltan esquemas en el warehouse: {sorted(missing)}")
        return sorted(EXPECTED_SCHEMAS)

    @task
    def check_project_package() -> str:
        import weather_etl

        return weather_etl.__file__

    check_warehouse_schemas()
    check_project_package()


warehouse_healthcheck()
