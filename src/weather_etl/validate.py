"""Validate: controles de calidad de datos antes de cargar al warehouse.

Recibe el DataFrame que produce transform.py y revisa que los datos tengan
sentido. Hay dos niveles:
- Error: el dato esta mal y no debe llegar al warehouse. Se junta la lista
  completa de errores y se lanza DataQualityError: el pipeline se detiene.
- Advertencia: falta un dato no critico. Se registra en el log y se carga igual.
"""

import logging
from datetime import date

import pandas as pd

from weather_etl.transform import DECIMAL_COLUMNS, STAGING_COLUMNS

logger = logging.getLogger(__name__)

KEY_COLUMNS = ["city_id", "weather_date"]

# Sin estos datos la fila no sirve: si faltan, es error.
REQUIRED_COLUMNS = [*KEY_COLUMNS, "temperature_max_c", "temperature_min_c", "precipitation_mm"]
# Utiles pero no criticos: si faltan, solo advertencia.
OPTIONAL_COLUMNS = ["temperature_mean_c", "wind_speed_max_kmh", "weather_code"]

# Rangos fisicamente posibles en la Tierra (con margen sobre los records mundiales).
# No buscan detectar clima "raro", sino datos imposibles: senal de un bug o de una API rota.
VALID_RANGES = {
    "temperature_max_c": (-90, 60),
    "temperature_min_c": (-90, 60),
    "temperature_mean_c": (-90, 60),
    "precipitation_mm": (0, 2000),
    "wind_speed_max_kmh": (0, 410),
}

# Codigos de clima WMO que usa Open-Meteo.
VALID_WEATHER_CODES = frozenset(
    [0, 1, 2, 3]  # despejado a nublado
    + [45, 48]  # niebla
    + [51, 53, 55, 56, 57]  # llovizna
    + [61, 63, 65, 66, 67]  # lluvia
    + [71, 73, 75, 77]  # nieve
    + [80, 81, 82, 85, 86]  # chubascos
    + [95, 96, 99]  # tormenta
)


class DataQualityError(Exception):
    """Los datos no pasaron los controles de calidad."""

    def __init__(self, errors: list[str]):
        self.errors = errors
        detail = "\n".join(f"  - {error}" for error in errors)
        super().__init__(f"{len(errors)} control(es) de calidad fallaron:\n{detail}")


def validate_daily_weather(
    df: pd.DataFrame,
    expected_city_ids: list[str] | None = None,
    expected_date: date | None = None,
) -> None:
    """Corre todos los controles. Lanza DataQualityError si hay al menos un error."""
    # Si el esquema esta mal, los demas controles no se pueden ni ejecutar.
    schema_errors = check_schema(df)
    if schema_errors:
        raise DataQualityError(schema_errors)

    errors = [
        *check_not_empty(df),
        *check_no_nulls(df, REQUIRED_COLUMNS),
        *check_unique_key(df),
        *check_value_ranges(df),
        *check_temperature_consistency(df),
        *check_weather_codes(df),
    ]
    if expected_city_ids is not None:
        errors += check_expected_cities(df, expected_city_ids)
    if expected_date is not None:
        errors += check_expected_date(df, expected_date)

    for warning in check_no_nulls(df, OPTIONAL_COLUMNS):
        logger.warning("Advertencia de calidad: %s", warning)

    if errors:
        raise DataQualityError(errors)

    logger.info("Validacion OK: %d filas pasaron todos los controles", len(df))


# --- Controles individuales ---------------------------------------------------------
# Cada uno devuelve una lista de mensajes de error (vacia = todo bien).
# Son funciones separadas para poder testear cada regla por si sola.


def check_schema(df: pd.DataFrame) -> list[str]:
    errors = []
    missing = [c for c in STAGING_COLUMNS if c not in df.columns]
    extra = [c for c in df.columns if c not in STAGING_COLUMNS]
    if missing:
        errors.append(f"Faltan columnas: {missing}")
    if extra:
        errors.append(f"Columnas inesperadas: {extra}")

    for column in DECIMAL_COLUMNS:
        if column in df.columns and not pd.api.types.is_float_dtype(df[column]):
            errors.append(f"{column} deberia ser decimal y es {df[column].dtype}")
    if "weather_code" in df.columns and not pd.api.types.is_integer_dtype(df["weather_code"]):
        errors.append(f"weather_code deberia ser entero y es {df['weather_code'].dtype}")
    return errors


def check_not_empty(df: pd.DataFrame) -> list[str]:
    return ["El DataFrame esta vacio"] if df.empty else []


def check_no_nulls(df: pd.DataFrame, columns: list[str]) -> list[str]:
    errors = []
    for column in columns:
        null_rows = df[df[column].isna()]
        if not null_rows.empty:
            errors.append(f"{column} tiene {len(null_rows)} nulo(s): {_describe(null_rows)}")
    return errors


def check_unique_key(df: pd.DataFrame) -> list[str]:
    duplicated = df[df.duplicated(subset=KEY_COLUMNS, keep=False)]
    if duplicated.empty:
        return []
    return [f"Filas duplicadas por {KEY_COLUMNS}: {_describe(duplicated)}"]


def check_value_ranges(df: pd.DataFrame) -> list[str]:
    errors = []
    for column, (low, high) in VALID_RANGES.items():
        values = df[column]
        # notna(): los nulos no son "fuera de rango"; de eso se encarga check_no_nulls.
        out_of_range = df[values.notna() & ~values.between(low, high)]
        if not out_of_range.empty:
            errors.append(
                f"{column} fuera del rango [{low}, {high}]: "
                f"{_describe(out_of_range, show_column=column)}"
            )
    return errors


def check_temperature_consistency(df: pd.DataFrame) -> list[str]:
    errors = []
    tmin, tmean, tmax = df["temperature_min_c"], df["temperature_mean_c"], df["temperature_max_c"]

    min_above_max = df[tmin > tmax]
    if not min_above_max.empty:
        errors.append(f"Temperatura minima mayor que la maxima: {_describe(min_above_max)}")

    mean_outside = df[(tmean < tmin) | (tmean > tmax)]
    if not mean_outside.empty:
        errors.append(f"Temperatura media fuera de [min, max]: {_describe(mean_outside)}")
    return errors


def check_weather_codes(df: pd.DataFrame) -> list[str]:
    codes = df["weather_code"]
    invalid = df[codes.notna() & ~codes.isin(VALID_WEATHER_CODES)]
    if invalid.empty:
        return []
    return [f"weather_code no valido: {_describe(invalid, show_column='weather_code')}"]


def check_expected_cities(df: pd.DataFrame, expected_city_ids: list[str]) -> list[str]:
    errors = []
    found = set(df["city_id"])
    missing = sorted(set(expected_city_ids) - found)
    unexpected = sorted(found - set(expected_city_ids))
    if missing:
        errors.append(f"Faltan ciudades: {missing}")
    if unexpected:
        errors.append(f"Ciudades que no estan en la configuracion: {unexpected}")
    return errors


def check_expected_date(df: pd.DataFrame, expected_date: date) -> list[str]:
    wrong_date = df[df["weather_date"] != expected_date]
    if wrong_date.empty:
        return []
    return [f"Filas con fecha distinta a {expected_date}: {_describe(wrong_date)}"]


def _describe(rows: pd.DataFrame, show_column: str | None = None, limit: int = 5) -> str:
    """Resume las filas con problema: 'santiago 2026-10-13, lima 2026-10-13 (+3 mas)'."""
    labels = []
    for _, row in rows.head(limit).iterrows():
        label = f"{row['city_id']} {row['weather_date']}"
        if show_column:
            label += f" ({show_column}={row[show_column]})"
        labels.append(label)
    remaining = len(rows) - limit
    return ", ".join(labels) + (f" (+{remaining} mas)" if remaining > 0 else "")


if __name__ == "__main__":
    # Prueba manual: uv run python -m weather_etl.validate
    from datetime import timedelta

    from weather_etl.extract import fetch_daily_weather, load_cities
    from weather_etl.transform import transform_responses

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    cities = load_cities()
    yesterday = date.today() - timedelta(days=1)
    df = transform_responses([fetch_daily_weather(city, yesterday) for city in cities])
    city_ids = [city["city_id"] for city in cities]

    logger.info("=== 1) Datos reales ===")
    validate_daily_weather(df, expected_city_ids=city_ids, expected_date=yesterday)

    logger.info("=== 2) Mismos datos, corrompidos a proposito ===")
    broken = pd.concat([df, df.head(1)], ignore_index=True)  # fila duplicada
    broken.loc[1, "temperature_min_c"] = 99.0  # minima imposible y mayor que la maxima
    broken.loc[2, "precipitation_mm"] = None  # dato obligatorio vacio
    broken.loc[3, "weather_code"] = 7  # codigo que no existe
    broken.loc[4, "wind_speed_max_kmh"] = None  # dato opcional vacio: solo advertencia
    try:
        validate_daily_weather(broken, expected_city_ids=city_ids, expected_date=yesterday)
    except DataQualityError as exc:
        logger.error("%s", exc)
