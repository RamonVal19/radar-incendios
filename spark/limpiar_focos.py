"""Job de Spark del rol A: genera la tabla `focos` (contrato schemas/esquemas.md, sección 2)
y calcula el umbral de `dia_muchos_focos` (percentil 90 del histórico).

Responsable: Ramón Valenzuela.

Qué hace:
1. Lee los CSV de FIRMS VIIRS S-NPP de Chile desde HDFS (/raw/firms/).
2. Se queda con los focos de vegetación (type = 0) dentro de la zona de estudio.
3. Junta acq_date + acq_time (UTC) y los pasa a hora de Chile (America/Santiago) para obtener `fecha`.
4. Deja solo las temporadas 2013 a 2025 (1-jul-2012 a 30-jun-2025), que son las que tienen clima.
5. Calcula `celda_id` y descarta los focos que caen fuera de la tabla `celdas`.
6. Guarda el resultado en /clean/focos/ (Parquet).
7. Calcula el percentil 90 de focos por celda y día en las temporadas 2013-2024, contando solo
   los días con al menos un foco (sin mirar 2025), y lo deja en data/umbral.txt.

Ejecutar (PowerShell, desde la carpeta del repo):
  docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/proyecto/spark/limpiar_focos.py 2>$null
"""
import argparse
import math

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

LAT_MIN, LAT_MAX = -39.6, -32.0      # zona de estudio
LON_MIN, LON_MAX = -74.0, -70.0
INICIO = "2012-07-01"                # temporada 2013
FIN = "2025-06-30"                   # temporada 2025
ULTIMA_TEMPORADA_HISTORICO = 2024
TAM = 0.25


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--entrada", default="hdfs://namenode:9000/raw/firms/*.csv")
    p.add_argument("--celdas", default="hdfs://namenode:9000/clean/celdas/")
    p.add_argument("--salida", default="hdfs://namenode:9000/clean/focos/")
    p.add_argument("--umbral", default="/opt/proyecto/data/umbral.txt")
    a = p.parse_args()

    spark = SparkSession.builder.appName("limpiar-focos").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    crudos = spark.read.csv(a.entrada, header=True, inferSchema=True)
    print(f"Filas crudas: {crudos.count()}")

    hora_utc = F.lpad(F.col("acq_time").cast("int").cast("string"), 4, "0")
    instante_utc = F.to_timestamp(F.concat_ws(" ", F.col("acq_date").cast("string"), hora_utc), "yyyy-MM-dd HHmm")
    lat_celda = F.floor(F.col("lat") / TAM) * TAM + TAM / 2
    lon_celda = F.floor(F.col("lon") / TAM) * TAM + TAM / 2

    focos = (crudos
             .filter(F.col("type").cast("int") == 0)
             .withColumn("lat", F.col("latitude").cast("double"))
             .withColumn("lon", F.col("longitude").cast("double"))
             .filter(F.col("lat").between(LAT_MIN, LAT_MAX) & F.col("lon").between(LON_MIN, LON_MAX))
             .withColumn("hora_utc", hora_utc)
             .withColumn("fecha", F.to_date(F.from_utc_timestamp(instante_utc, "America/Santiago")))
             .filter(F.col("fecha").between(INICIO, FIN))
             .withColumn("celda_id", F.format_string("%.3f_%.3f", lat_celda, lon_celda))
             .select("fecha", "hora_utc", "lat", "lon", "celda_id",
                     F.col("frp").cast("double").alias("frp"),
                     F.col("confidence").cast("string").alias("confidence"),
                     F.col("daynight").cast("string").alias("daynight")))

    celdas = spark.read.parquet(a.celdas).select("celda_id")
    antes = focos.count()
    focos = focos.join(F.broadcast(celdas), "celda_id", "inner") \
                 .select("fecha", "hora_utc", "lat", "lon", "celda_id", "frp", "confidence", "daynight")
    focos.write.mode("overwrite").parquet(a.salida)

    focos = spark.read.parquet(a.salida)
    despues = focos.count()
    print(f"Focos de vegetación en la zona, temporadas 2013-2025: {antes}")
    print(f"Focos dentro de las celdas (guardados): {despues} | descartados fuera de celdas: {antes - despues}")

    temporada = F.when(F.month("fecha") >= 7, F.year("fecha") + 1).otherwise(F.year("fecha"))
    focos = focos.withColumn("temporada", temporada)
    focos.groupBy("temporada").count().orderBy("temporada").show(20)

    # Umbral: percentil 90 de focos por celda y día (solo días con focos) en el histórico.
    por_dia = (focos.filter(F.col("temporada") <= ULTIMA_TEMPORADA_HISTORICO)
               .groupBy("celda_id", "fecha").agg(F.count("*").alias("n_focos")))
    fila = por_dia.agg(F.expr("percentile(n_focos, 0.9)").alias("p90"),
                       F.count("*").alias("celda_dias"),
                       F.avg("n_focos").alias("promedio"),
                       F.max("n_focos").alias("maximo")).first()
    umbral = math.ceil(fila.p90)
    print(f"Celda-días con focos (2013-2024): {fila.celda_dias} | promedio {fila.promedio:.2f} | máximo {fila.maximo}")
    print(f"Percentil 90: {fila.p90} -> UMBRAL dia_muchos_focos = n_focos >= {umbral}")

    with open(a.umbral, "w", encoding="utf-8") as f:
        f.write(f"p90={fila.p90}\numbral={umbral}\n")
    print(f"-> {a.salida} y {a.umbral}")
    spark.stop()


if __name__ == "__main__":
    main()
