"""Job de Spark del rol A: genera la tabla fija `celdas` (contrato schemas/esquemas.md, sección 1).

Responsable: Ramón Valenzuela.

Qué hace:
1. Lee los CSV de FIRMS VIIRS S-NPP de Chile desde HDFS (/raw/firms/).
2. Se queda con los focos de vegetación (type = 0) de las temporadas 2013-2024
   (1-jul-2012 a 30-jun-2024) dentro de la zona de estudio.
3. Calcula la celda de 0,25° de cada foco y se queda con las celdas distintas.
4. Agrega unas 30 celdas de control: celdas de la zona de estudio, en territorio chileno,
   que nunca tuvieron focos en el histórico, elegidas al azar con semilla fija.
5. Guarda el resultado en data/celdas.csv (para el repo y el script de descarga del clima)
   y en /clean/celdas/ (Parquet, para los demás jobs y Hive).

Ejecutar (PowerShell, desde la carpeta del repo):
  docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/proyecto/spark/generar_celdas.py
"""
import csv
import math
import random

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Parámetros
# ---------------------------------------------------------------------------
ENTRADA_FIRMS = "hdfs://namenode:9000/raw/firms/*.csv"
SALIDA_CSV = "/opt/proyecto/data/celdas.csv"          # archivo local (carpeta del repo)
SALIDA_PARQUET = "hdfs://namenode:9000/clean/celdas/"

LAT_MIN, LAT_MAX = -39.6, -32.0      # zona de estudio
LON_MIN, LON_MAX = -74.0, -70.0
INICIO_HISTORICO = "2012-07-01"      # temporada 2013
FIN_HISTORICO = "2024-06-30"         # temporada 2024
N_CONTROL = 30
SEMILLA = 2025
TAM = 0.25                           # tamaño de la celda en grados
MARGEN = 0.125                       # distancia mínima (media celda) a la costa y a la frontera para las celdas de control

# Contorno aproximado de Chile continental en la zona de estudio: (latitud, longitud de la costa,
# longitud de la frontera con Argentina). Solo se usa para elegir celdas de control en tierra
# chilena. Entre dos latitudes se interpola en línea recta.
CONTORNO = [
    (-32.0, -71.52, -70.20),
    (-33.0, -71.65, -70.00),
    (-34.0, -71.95, -70.00),
    (-35.0, -72.20, -70.40),
    (-36.0, -72.80, -70.60),
    (-37.0, -73.20, -71.10),
    (-37.5, -73.65, -71.10),
    (-38.0, -73.50, -71.05),
    (-39.0, -73.20, -71.40),
    (-39.6, -73.25, -71.60),
]


def centro(v):
    """Centro de la celda según el contrato: floor(v / 0.25) * 0.25 + 0.125."""
    return math.floor(v / TAM) * TAM + TAM / 2


def en_chile(lat, lon):
    """True si (lat, lon) queda entre la costa y la frontera, con un margen de seguridad."""
    puntos = sorted(CONTORNO)                       # de sur a norte
    for (la1, co1, fr1), (la2, co2, fr2) in zip(puntos, puntos[1:]):
        if la1 <= lat <= la2:
            t = (lat - la1) / (la2 - la1)
            costa = co1 + t * (co2 - co1)
            frontera = fr1 + t * (fr2 - fr1)
            return costa + MARGEN <= lon <= min(frontera, LON_MAX) - MARGEN
    return False


def main():
    spark = SparkSession.builder.appName("generar-celdas").getOrCreate()

    focos = (spark.read.csv(ENTRADA_FIRMS, header=True, inferSchema=True)
             .filter(F.col("type") == 0)
             .filter(F.col("latitude").between(LAT_MIN, LAT_MAX))
             .filter(F.col("longitude").between(LON_MIN, LON_MAX))
             .filter(F.col("acq_date").cast("date").between(INICIO_HISTORICO, FIN_HISTORICO)))
    print(f"Focos de vegetación en la zona, temporadas 2013-2024: {focos.count()}")

    con_focos = (focos
                 .select((F.floor(F.col("latitude") / TAM) * TAM + TAM / 2).alias("lat_celda"),
                         (F.floor(F.col("longitude") / TAM) * TAM + TAM / 2).alias("lon_celda"))
                 .distinct()
                 .collect())
    celdas_focos = {(round(r.lat_celda, 3), round(r.lon_celda, 3)) for r in con_focos}
    print(f"Celdas con al menos un foco: {len(celdas_focos)}")

    # Candidatas a control: toda la grilla de la zona, en tierra chilena y sin focos.
    candidatas = []
    lat = centro(LAT_MIN)
    while lat < LAT_MAX:
        lon = centro(LON_MIN)
        while lon < LON_MAX:
            c = (round(lat, 3), round(lon, 3))
            if c not in celdas_focos and en_chile(*c):
                candidatas.append(c)
            lon += TAM
        lat += TAM
    random.seed(SEMILLA)
    control = sorted(random.sample(candidatas, min(N_CONTROL, len(candidatas))))
    print(f"Candidatas a control: {len(candidatas)} | elegidas: {len(control)}")

    filas = [(la, lo, False) for la, lo in sorted(celdas_focos)] + [(la, lo, True) for la, lo in control]
    filas = [{"celda_id": f"{la:.3f}_{lo:.3f}", "lat_celda": la, "lon_celda": lo,
              "zona": math.floor(la), "es_control": ctrl} for la, lo, ctrl in filas]

    with open(SALIDA_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["celda_id", "lat_celda", "lon_celda", "zona", "es_control"],
                           lineterminator="\n")
        w.writeheader()
        for fila in filas:
            w.writerow({**fila, "es_control": "true" if fila["es_control"] else "false"})

    df = spark.createDataFrame(filas, "celda_id STRING, lat_celda DOUBLE, lon_celda DOUBLE, zona INT, es_control BOOLEAN")
    df.write.mode("overwrite").parquet(SALIDA_PARQUET)

    print(f"TOTAL celdas: {len(filas)} ({len(celdas_focos)} con focos + {len(control)} de control)")
    print(f"-> {SALIDA_CSV} y {SALIDA_PARQUET}")
    df.groupBy("zona", "es_control").count().orderBy("zona", "es_control").show(40)
    spark.stop()


if __name__ == "__main__":
    main()
