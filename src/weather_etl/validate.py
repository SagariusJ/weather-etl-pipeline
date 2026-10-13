"""Validate: controles de calidad antes de cargar.

Responsabilidades:
- Esquema: columnas y tipos esperados.
- Nulos en campos obligatorios, duplicados por (city_id, weather_date).
- Rangos fisicamente razonables (temperaturas, precipitacion >= 0, max >= min).
- Si algo falla, levantar una excepcion: el pipeline debe detenerse, no cargar basura.

TODO (paso 7)
"""
