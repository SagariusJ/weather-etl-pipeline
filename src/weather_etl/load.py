"""Load: escribe en el warehouse de forma idempotente.

Responsabilidades:
- Insertar las respuestas crudas en raw.weather_api_responses.
- Upsert en staging.daily_weather (INSERT ... ON CONFLICT DO UPDATE).
- Mantener analytics.dim_city sincronizada con config/cities.yaml.

TODO (paso 8)
"""
