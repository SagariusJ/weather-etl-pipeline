"""Tests de extract.py, sin internet: la API se reemplaza por respuestas falsas."""

from datetime import date

import pytest
import requests

from weather_etl import extract

CITY = {
    "city_id": "santiago",
    "latitude": -33.45,
    "longitude": -70.67,
    "timezone": "America/Santiago",
}
DAY = date(2026, 10, 1)


class FakeResponse:
    """Imita lo minimo de requests.Response que usa extract.py."""

    def __init__(self, status_code: int, body: dict):
        self.status_code = status_code
        self._body = body
        self.url = "https://example.test/archive"
        self.text = str(body)

    def json(self) -> dict:
        return self._body


@pytest.fixture
def fake_api(monkeypatch):
    """Reemplaza requests.get por una lista de respuestas preparadas.

    Cada llamada consume la siguiente: una respuesta o una excepcion a lanzar.
    Tambien anula time.sleep para que los tests de reintentos no esperen de verdad.
    """
    calls = []

    def setup(*responses):
        queue = list(responses)

        def fake_get(url, params, timeout):
            calls.append({"url": url, "params": params, "timeout": timeout})
            result = queue.pop(0)
            if isinstance(result, Exception):
                raise result
            return result

        monkeypatch.setattr(extract.requests, "get", fake_get)
        monkeypatch.setattr(extract.time, "sleep", lambda seconds: None)
        return calls

    return setup


def test_request_params_ask_for_one_local_day():
    params = extract.build_request_params(CITY, DAY)
    assert params["start_date"] == params["end_date"] == "2026-10-01"
    assert params["timezone"] == "America/Santiago"
    assert params["daily"].split(",") == extract.DAILY_VARIABLES


def test_success_returns_raw_payload_untouched(fake_api):
    body = {"daily": {"time": ["2026-10-01"]}}
    calls = fake_api(FakeResponse(200, body))

    result = extract.fetch_daily_weather(CITY, DAY)

    assert result["payload"] == body
    assert result["city_id"] == "santiago"
    assert result["logical_date"] == DAY
    assert calls[0]["timeout"] == extract.REQUEST_TIMEOUT_SECONDS


def test_server_error_is_retried_until_success(fake_api):
    calls = fake_api(
        FakeResponse(503, {}),
        requests.exceptions.ConnectionError("red caida"),
        FakeResponse(200, {"daily": {}}),
    )
    extract.fetch_daily_weather(CITY, DAY, max_retries=3)
    assert len(calls) == 3


def test_client_error_is_not_retried(fake_api):
    calls = fake_api(FakeResponse(400, {"error": True, "reason": "Latitude must be in range"}))

    with pytest.raises(extract.ExtractError, match="Latitude must be in range"):
        extract.fetch_daily_weather(CITY, DAY, max_retries=3)

    assert len(calls) == 1  # un 400 no se arregla reintentando


def test_gives_up_after_max_retries(fake_api):
    calls = fake_api(*[requests.exceptions.Timeout("lento")] * 3)

    with pytest.raises(extract.ExtractError, match="tras 3 intentos"):
        extract.fetch_daily_weather(CITY, DAY, max_retries=3)

    assert len(calls) == 3


def test_load_cities_reads_repo_config():
    cities = extract.load_cities(extract.DEFAULT_CONFIG_PATH)
    assert len(cities) >= 1
    assert all("city_id" in city for city in cities)
