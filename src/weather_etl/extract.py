"""Extract: obtiene los datos diarios de la API de Open-Meteo.

Responsabilidades:
- Leer la lista de ciudades desde config/cities.yaml.
- Llamar a la API para una fecha dada (la logical_date del DAG).
- Reintentar con backoff ante errores transitorios (timeouts, HTTP 429/5xx).
- Devolver la respuesta cruda (JSON) sin modificarla, para guardarla en raw.

TODO (paso 5)
"""
