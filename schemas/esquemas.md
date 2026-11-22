# Contrato de datos (versión 3.1, 22-nov-2026)

Este archivo fija cómo se ven los datos entre una etapa y otra, para que cada integrante trabaje en paralelo con datos de muestra. Cualquier cambio se avisa al grupo antes de hacerlo.

## Roles
| rol | responsable | produce |
|---|---|---|
| A | Ramón | `celdas`, `focos`, coordinación y ejecución final |
| B | Alan (descarga) y Daniel (`clima_diario.py`) | `clima_horario`, `clima_diario` |
| C | Alan | `resultado`, `evaluacion_regla` |
| D | Matías | Tablas Hive, índices de Elasticsearch, mapa en Kibana y README |

El detalle de tareas y fechas de cada integrante está en el documento de tareas de la semana.

## Reglas comunes
- **Zona de estudio:** Latitud entre -39,6 y -32,0, longitud entre -74,0 y -70,0 (centro-sur continental, sin Argentina ni islas).
- **Fechas:** Hora local de Chile (America/Santiago), formato AAAA-MM-DD.
- **Grilla:** Celdas de 0,25°. `celda_id` es el centro de la celda como texto `"<lat>_<lon>"` con 3 decimales, por ejemplo `-36.875_-72.375`. El centro se calcula como `floor(v / 0.25) * 0.25 + 0.125` y el texto con `format_string('%.3f_%.3f', lat, lon)`.
- **Zona:** Banda de 1° de latitud, `zona = floor(lat_celda)`, por ejemplo -37. Se usa para particionar en Hive.
- **Temporada:** Va del 1 de julio al 30 de junio y lleva el año en que termina. La temporada 2025 va del 1-jul-2024 al 30-jun-2025.
- **Histórico y validación:** Histórico = temporadas 2013 a 2024. Validación = temporada 2025. Ningún umbral se ajusta mirando la temporada 2025.
- **Formato:** CSV con encabezado, coma como separador y punto decimal en `data/sample/`. Parquet en HDFS.

## 1. celdas (rol A, se congela una vez)
Lista fija de celdas que se usan en todo el proyecto. Se genera una sola vez desde los focos de las temporadas 2013-2024 y se sube a `data/celdas.csv` (no a `data/sample/`, que guarda la muestra de 5 celdas) y a `/clean/celdas/`. Rol B descarga el clima de estas celdas.

| columna | tipo | descripción |
|---|---|---|
| celda_id | string | Celda de 0,25° |
| lat_celda, lon_celda | double | Centro de la celda |
| zona | int | Banda de latitud |
| es_control | boolean | true si la celda nunca tuvo focos en el histórico (elegida al azar dentro de la zona de estudio y en territorio chileno, unas 30 celdas) |

## 2. focos (rol A)
Un registro por detección VIIRS S-NPP de vegetación (`type = 0`) dentro de la zona de estudio.

| columna | tipo | descripción |
|---|---|---|
| fecha | date | Fecha local de la detección |
| hora_utc | string | Hora HHMM en UTC con 4 dígitos (rellenar con ceros a la izquierda) |
| lat, lon | double | Coordenadas del foco |
| celda_id | string | Celda de 0,25° |
| frp | double | Potencia radiativa del fuego (MW) |
| confidence | string | Confianza de la detección: l (baja), n (normal) o h (alta) |
| daynight | string | D (día) o N (noche) |

## 3. clima_horario (rol B)
Un registro por celda y hora, desde la API histórica de Open-Meteo con `timezone=America/Santiago` y `wind_speed_unit=kmh`.

| columna | tipo | descripción |
|---|---|---|
| fecha | date | Fecha local |
| hora | int | Hora local (0-23) |
| celda_id | string | Celda de 0,25° |
| temp | double | Temperatura a 2 m (°C), `temperature_2m` |
| hr | double | Humedad relativa a 2 m (%), `relative_humidity_2m` |
| viento | double | Viento medio a 10 m (km/h), `wind_speed_10m` |
| rafaga | double | Ráfaga a 10 m (km/h), `wind_gusts_10m` |
| lluvia_hora | double | Precipitación de la hora (mm), `precipitation` |

En el cambio de horario puede repetirse o faltar una hora: no afecta, porque todo se resume por `fecha`.

## 4. clima_diario (rol B, lo calcula Spark desde clima_horario)
Un registro por celda y día.

| columna | tipo | descripción |
|---|---|---|
| fecha | date | Fecha local |
| celda_id | string | Celda de 0,25° |
| tmax | double | Temperatura máxima del día (°C) |
| hr_min | double | Humedad relativa mínima del día (%) |
| viento_max | double | Viento medio máximo del día (km/h) |
| rafaga_max | double | Ráfaga máxima del día (km/h) |
| lluvia_dia | double | Precipitación total del día (mm) |
| horas_regla | int | Horas del día con temp > 30, hr < 30 y viento > 30 a la vez |
| horas_regla_rafaga | int | Igual, pero con rafaga > 30 en vez de viento |

**Plan B (se decide el lunes 23-nov según la cuota de Open-Meteo):** Se baja el clima diario de Open-Meteo con las mismas columnas, y `horas_regla` y `horas_regla_rafaga` quedan en null. Nada más cambia aguas abajo.

## 5. resultado (rol C, lo consume rol D)
Un registro por celda y día para todas las celdas de `celdas`, con o sin focos.

| columna | tipo | descripción |
|---|---|---|
| fecha | date | Fecha local |
| anio | int | Año calendario (partición) |
| temporada | int | Temporada jul-jun |
| es_validacion | boolean | true si temporada = 2025 |
| celda_id, lat_celda, lon_celda, zona, es_control | | Copiados de `celdas` |
| n_focos | int | Focos de la celda ese día (0 si no hubo) |
| frp_total | double | Suma de frp (0 si no hubo focos) |
| tmax, hr_min, viento_max, rafaga_max, lluvia_dia | double | Copiados de `clima_diario` |
| tmax_d1, hr_min_d1, rafaga_max_d1 | double | Los mismos valores del día anterior (`lag` en Spark) |
| dias_sin_lluvia | int | Días seguidos con lluvia_dia < 1 mm hasta ese día. Vale 0 si ese día llovió |
| regla_diaria | boolean | tmax > 30 y hr_min < 30 y viento_max > 30 (máximos del día) |
| regla_simultanea | boolean | horas_regla >= 1 (las tres condiciones en la misma hora) |
| regla_rafaga | boolean | horas_regla_rafaga >= 1 |
| regla_diaria_d1 | boolean | regla_diaria del día anterior (para anticipar) |
| dia_muchos_focos | boolean | n_focos >= umbral. Umbral provisorio 3. Se fija el 25-nov como el percentil 90 de n_focos (días con focos) del histórico, sin mirar 2025 |

## 6. evaluacion_regla (rol C)
Un registro por versión de la regla y temporada. Responde la pregunta de la propuesta.

| columna | tipo | descripción |
|---|---|---|
| version | string | diaria, simultanea, rafaga o diaria_d1 |
| temporada | int | Temporada evaluada |
| dias_alerta | int | Celda-días con la regla en true |
| aciertos | int | Alerta y dia_muchos_focos |
| falsas_alarmas | int | Alerta y n_focos = 0 |
| omisiones | int | dia_muchos_focos sin alerta |
| focos_en_alerta | int | Focos ocurridos en celda-días con alerta |
| focos_totales | int | Focos de la temporada |
| tasa_base | double | Proporción de celda-días con dia_muchos_focos (para comparar contra la regla) |

## Rutas y nombres
- **HDFS:** `/raw/firms/`, `/raw/open-meteo/`, `/clean/celdas/`, `/clean/focos/`, `/clean/clima_horario/`, `/clean/clima_diario/`, `/clean/resultado/`, `/clean/evaluacion_regla/` (Parquet).
- **Hive:** Base `radar`, una tabla por sección con el mismo nombre. `resultado` se particiona por `anio` y `zona`.
- **Elasticsearch:** Índice `focos` y índice `resultado`, cada uno con un campo `ubicacion` de tipo `geo_point` (`{lat, lon}`) y `fecha` de tipo `date`.
- **Muestras:** `data/sample/<tabla>.csv`, unas 100 filas por tabla, con estas mismas columnas.

## Reglas de limpieza de focos (job de Spark)
1. Zona de estudio: latitud y longitud según las reglas comunes.
2. Solo vegetación: `type = 0`.
3. Fecha: `acq_date` + `acq_time` (UTC) convertidas a America/Santiago.
4. `celda_id`: con la fórmula de arriba.

## Lectura de Open-Meteo
El CSV trae 3 líneas de metadatos antes de la tabla: hay que saltarlas. La columna `time` viene como `AAAA-MM-DDTHH:MM` en hora local, y de ahí salen `fecha` y `hora`.
