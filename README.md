# Radar de incendios forestales en Chile

Proyecto Big Data

Integrantes: Matías Vera, Alan Caro, Ramón Valenzuela y Daniel Herrero.

## Datos crudos (data/raw/)

| Archivo | Fuente | Contenido |
|---|---|---|
| `firms/viirs-snpp_2023_Chile.csv` | NASA FIRMS, descarga anual por país: https://firms.modaps.eosdis.nasa.gov/country/ (VIIRS S-NPP, 2023, Chile) | Focos de calor detectados por satélite en Chile durante 2023, sin filtrar |
| `open-meteo/open-meteo-36.87S72.36W96m.csv` | Open-Meteo, API histórica: https://archive-api.open-meteo.com/v1/archive | Clima horario de febrero 2023 (temperatura, humedad, viento, ráfagas y lluvia) en la celda -36.875_-72.375 de Ñuble |

Los archivos se guardan tal como se descargaron. La limpieza se hace en Spark y las tablas resultantes se describen en `schemas/esquemas.md`.
