"""Extract: obtiene los datos diarios de la API de Open-Meteo.

Una llamada por ciudad y dia. Devuelve la respuesta cruda (sin modificar)
para guardarla en raw.weather_api_responses.
"""

import json
import logging
import os
import time
from datetime import date, timedelta
from pathlib import Path

import requests
import yaml

logger = logging.getLogger(__name__)

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Variables diarias que pedimos. Cada una termina siendo una columna de
# staging.daily_weather (transform.py se encarga de renombrarlas).
DAILY_VARIABLES = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "wind_speed_10m_max",
    "weather_code",
]

REQUEST_TIMEOUT_SECONDS = 30

# Codigos HTTP que indican un problema temporal del servidor: vale la pena reintentar.
# 429 = demasiadas peticiones, 5xx = el servidor fallo.
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# <repo>/config/cities.yaml. Se usa al correr en tu PC; en Docker manda la variable
# de entorno WEATHER_CITIES_CONFIG (definida en docker-compose.yml).
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "cities.yaml"


class ExtractError(Exception):
    """No se pudo obtener datos de la API."""


def load_cities(config_path: str | Path | None = None) -> list[dict]:
    """Lee la lista de ciudades desde el YAML de configuracion."""
    path = Path(config_path or os.environ.get("WEATHER_CITIES_CONFIG", DEFAULT_CONFIG_PATH))
    with path.open(encoding="utf-8") as f:
        config = yaml.safe_load(f)

    cities = config["cities"]
    logger.info("Cargadas %d ciudades desde %s", len(cities), path)
    return cities


def build_request_params(city: dict, day: date) -> dict:
    """Arma los parametros de la API para una ciudad y un dia."""
    return {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "start_date": day.isoformat(),
        "end_date": day.isoformat(),
        "daily": ",".join(DAILY_VARIABLES),
        # Zona horaria de la ciudad: asi "el dia" es el dia local, no el de UTC.
        "timezone": city["timezone"],
    }


def fetch_daily_weather(city: dict, day: date, max_retries: int = 3) -> dict:
    """Llama a la API para una ciudad y un dia, reintentando ante fallos temporales.

    Devuelve un dict listo para insertar en raw.weather_api_responses.
    Lanza ExtractError si la peticion es invalida o si se agotan los reintentos.
    """
    city_id = city["city_id"]
    params = build_request_params(city, day)

    for attempt in range(1, max_retries + 1):
        logger.info(
            "Pidiendo clima de %s para %s (intento %d/%d)", city_id, day, attempt, max_retries
        )
        try:
            response = requests.get(ARCHIVE_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            # Problema de red: temporal, se reintenta.
            error = f"error de red: {exc}"
        else:
            if response.status_code == 200:
                return {
                    "city_id": city_id,
                    "logical_date": day,
                    "request_url": response.url,
                    "payload": response.json(),
                }
            if response.status_code not in RETRYABLE_STATUS_CODES:
                # 400, 404...: la peticion esta mal hecha. Reintentar daria el mismo error.
                raise ExtractError(
                    f"La API rechazo la peticion de {city_id} "
                    f"(HTTP {response.status_code}): {_error_reason(response)}"
                )
            error = f"HTTP {response.status_code}"

        if attempt < max_retries:
            wait_seconds = 2 ** (attempt - 1)  # backoff exponencial: 1s, 2s, 4s...
            logger.warning(
                "Fallo temporal para %s (%s). Reintentando en %ds", city_id, error, wait_seconds
            )
            time.sleep(wait_seconds)

    raise ExtractError(
        f"No se pudo obtener el clima de {city_id} para {day} "
        f"tras {max_retries} intentos. Ultimo error: {error}"
    )


def _error_reason(response: requests.Response) -> str:
    """Extrae el mensaje de error que manda Open-Meteo en el campo 'reason'."""
    try:
        return response.json().get("reason", response.text)
    except ValueError:  # la respuesta no era JSON
        return response.text


if __name__ == "__main__":
    # Prueba manual: uv run python -m weather_etl.extract
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    santiago = load_cities()[0]
    yesterday = date.today() - timedelta(days=1)
    result = fetch_daily_weather(santiago, yesterday)

    print(f"\nURL: {result['request_url']}")
    print(json.dumps(result["payload"]["daily"], indent=2, ensure_ascii=False))
