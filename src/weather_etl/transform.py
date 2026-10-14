"""Transform: convierte las respuestas crudas de la API en filas limpias de staging.

Entrada: lista de dicts que devuelve extract.fetch_daily_weather.
Salida: DataFrame con una fila por (city_id, weather_date) y las mismas
columnas que staging.daily_weather.

Este modulo solo da forma y tipos a los datos. Revisar si los valores
tienen sentido (nulos, rangos, duplicados) es trabajo de validate.py.
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)

# Nombre en la API -> nombre en staging.daily_weather
COLUMN_MAPPING = {
    "time": "weather_date",
    "temperature_2m_max": "temperature_max_c",
    "temperature_2m_min": "temperature_min_c",
    "temperature_2m_mean": "temperature_mean_c",
    "precipitation_sum": "precipitation_mm",
    "wind_speed_10m_max": "wind_speed_max_kmh",
    "weather_code": "weather_code",
}

# Unidades que esperamos. Los nombres de columna las llevan (_c, _mm, _kmh),
# asi que si la API cambiara de unidad, los datos quedarian mal sin avisar.
EXPECTED_UNITS = {
    "temperature_2m_max": "°C",
    "temperature_2m_min": "°C",
    "temperature_2m_mean": "°C",
    "precipitation_sum": "mm",
    "wind_speed_10m_max": "km/h",
}

STAGING_COLUMNS = ["city_id", *COLUMN_MAPPING.values()]

DECIMAL_COLUMNS = [
    "temperature_max_c",
    "temperature_min_c",
    "temperature_mean_c",
    "precipitation_mm",
    "wind_speed_max_kmh",
]


class TransformError(Exception):
    """La respuesta de la API no tiene la estructura esperada."""


def transform_response(raw: dict) -> pd.DataFrame:
    """Convierte la respuesta de UNA ciudad en un DataFrame (una fila por dia)."""
    city_id = raw["city_id"]
    payload = raw["payload"]

    daily = payload.get("daily")
    if not daily:
        raise TransformError(f"La respuesta de {city_id} no trae el bloque 'daily'")

    missing = set(COLUMN_MAPPING) - set(daily)
    if missing:
        raise TransformError(f"A la respuesta de {city_id} le faltan campos: {sorted(missing)}")

    units = payload.get("daily_units", {})
    for field, expected in EXPECTED_UNITS.items():
        if units.get(field) != expected:
            raise TransformError(
                f"Unidad inesperada en {city_id}.{field}: {units.get(field)!r} "
                f"(se esperaba {expected!r})"
            )

    # La API manda listas paralelas: {"time": [d1, d2], "temperature_2m_max": [t1, t2]}.
    # pd.DataFrame las convierte directo en columnas: fila 0 = d1/t1, fila 1 = d2/t2.
    df = pd.DataFrame({field: daily[field] for field in COLUMN_MAPPING})
    df = df.rename(columns=COLUMN_MAPPING)
    df.insert(0, "city_id", city_id)
    return df


def transform_responses(raw_responses: list[dict]) -> pd.DataFrame:
    """Transforma las respuestas de todas las ciudades en un solo DataFrame tipado."""
    if not raw_responses:
        logger.warning("No hay respuestas para transformar")
        return _normalize_types(pd.DataFrame(columns=STAGING_COLUMNS))

    frames = [transform_response(raw) for raw in raw_responses]
    df = pd.concat(frames, ignore_index=True)
    df = _normalize_types(df)

    logger.info("Transformadas %d filas de %d ciudades", len(df), len(raw_responses))
    return df[STAGING_COLUMNS]


def _normalize_types(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte cada columna al tipo que espera la tabla de staging."""
    df = df.copy()
    df["weather_date"] = pd.to_datetime(df["weather_date"], format="%Y-%m-%d").dt.date
    for column in DECIMAL_COLUMNS:
        # errors="raise": si llega texto donde va un numero, mejor fallar aqui.
        # astype("float64"): si la API manda 10 en vez de 10.0, pandas crearia una
        # columna de enteros; forzamos float para que el tipo sea siempre el mismo.
        df[column] = pd.to_numeric(df[column], errors="raise").astype("float64").round(2)
    # Int64 (con mayuscula) es el entero de pandas que admite nulos.
    df["weather_code"] = df["weather_code"].astype("Int64")
    return df


if __name__ == "__main__":
    # Prueba manual: uv run python -m weather_etl.transform
    from datetime import date, timedelta

    from weather_etl.extract import fetch_daily_weather, load_cities

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    yesterday = date.today() - timedelta(days=1)
    raw_responses = [fetch_daily_weather(city, yesterday) for city in load_cities()]
    result = transform_responses(raw_responses)

    print()
    print(result.to_string(index=False))
    print()
    print(result.dtypes)
