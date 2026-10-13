"""Valida que config/cities.yaml tenga la forma que espera el pipeline."""

from pathlib import Path

import yaml

CONFIG_PATH = Path(__file__).parents[1] / "config" / "cities.yaml"
REQUIRED_KEYS = {"city_id", "city_name", "country", "latitude", "longitude", "timezone"}


def load_cities() -> list[dict]:
    return yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))["cities"]


def test_cities_have_required_keys():
    for city in load_cities():
        assert REQUIRED_KEYS <= city.keys(), f"Faltan campos en {city}"


def test_city_ids_are_unique():
    ids = [city["city_id"] for city in load_cities()]
    assert len(ids) == len(set(ids))


def test_coordinates_are_valid():
    for city in load_cities():
        assert -90 <= city["latitude"] <= 90
        assert -180 <= city["longitude"] <= 180
