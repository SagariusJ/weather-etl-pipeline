-- Modelo del warehouse. Se ejecuta automaticamente la primera vez que se
-- crea el contenedor warehouse-db (volumen vacio).
--
--   raw        -> respuestas de la API tal como llegan (append-only)
--   staging    -> datos limpios: una fila por ciudad y dia
--   analytics  -> tablas/vistas listas para consumo
--   audit      -> trazabilidad de cada ejecucion del pipeline

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS audit;

-- RAW --------------------------------------------------------------------
-- Append-only: cada extraccion queda guardada, aunque se re-ejecute el dia.
-- Permite reprocesar staging sin volver a llamar a la API.
CREATE TABLE IF NOT EXISTS raw.weather_api_responses (
    id              BIGSERIAL    PRIMARY KEY,
    city_id         TEXT         NOT NULL,
    logical_date    DATE         NOT NULL,   -- dia que se esta procesando
    request_url     TEXT         NOT NULL,
    payload         JSONB        NOT NULL,
    ingested_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_raw_weather_city_date
    ON raw.weather_api_responses (city_id, logical_date);

-- STAGING ----------------------------------------------------------------
-- La PK (city_id, weather_date) es lo que hace posible el upsert idempotente:
-- re-ejecutar un dia actualiza las filas en vez de duplicarlas.
CREATE TABLE IF NOT EXISTS staging.daily_weather (
    city_id              TEXT          NOT NULL,
    weather_date         DATE          NOT NULL,
    temperature_max_c    NUMERIC(5,2),
    temperature_min_c    NUMERIC(5,2),
    temperature_mean_c   NUMERIC(5,2),
    precipitation_mm     NUMERIC(6,2)  CHECK (precipitation_mm >= 0),
    wind_speed_max_kmh   NUMERIC(6,2)  CHECK (wind_speed_max_kmh >= 0),
    weather_code         SMALLINT,
    loaded_at            TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (city_id, weather_date),
    CHECK (temperature_max_c >= temperature_min_c)
);

-- ANALYTICS --------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_city (
    city_id      TEXT           PRIMARY KEY,
    city_name    TEXT           NOT NULL,
    country      TEXT           NOT NULL,
    latitude     NUMERIC(8,5)   NOT NULL,
    longitude    NUMERIC(8,5)   NOT NULL,
    timezone     TEXT           NOT NULL,
    updated_at   TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE OR REPLACE VIEW analytics.weather_monthly_summary AS
SELECT
    c.city_id,
    c.city_name,
    c.country,
    date_trunc('month', w.weather_date)::date               AS month,
    count(*)                                                AS days_observed,
    round(avg(w.temperature_max_c), 1)                      AS avg_temp_max_c,
    round(avg(w.temperature_min_c), 1)                      AS avg_temp_min_c,
    max(w.temperature_max_c)                                AS highest_temp_c,
    min(w.temperature_min_c)                                AS lowest_temp_c,
    round(sum(w.precipitation_mm), 1)                       AS total_precipitation_mm,
    count(*) FILTER (WHERE w.precipitation_mm >= 1)         AS rainy_days,
    max(w.wind_speed_max_kmh)                               AS max_wind_speed_kmh
FROM staging.daily_weather AS w
JOIN analytics.dim_city   AS c USING (city_id)
GROUP BY c.city_id, c.city_name, c.country, date_trunc('month', w.weather_date);

-- AUDIT ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    id              BIGSERIAL     PRIMARY KEY,
    dag_id          TEXT          NOT NULL,
    run_id          TEXT          NOT NULL,
    logical_date    DATE          NOT NULL,
    status          TEXT          NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    rows_extracted  INTEGER,
    rows_loaded     INTEGER,
    error_message   TEXT,
    started_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_audit_runs_dag_date
    ON audit.pipeline_runs (dag_id, logical_date);
