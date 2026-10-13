"""Transform: convierte las respuestas crudas en filas limpias de staging.

Responsabilidades:
- Aplanar el JSON de Open-Meteo a una fila por (city_id, weather_date).
- Renombrar columnas al esquema de staging.daily_weather y normalizar tipos.

TODO (paso 6)
"""
