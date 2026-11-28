"""Job de Spark del rol B: arma clima_horario desde los CSV de Open-Meteo y lo resume en clima_diario.

Responsable: Daniel Herrero. Contrato: schemas/esquemas.md, secciones 3 y 4.

Entrada (modo normal): una carpeta con los CSV de Open-Meteo tal como vienen de la API
(format=csv). Cada archivo trae 3 líneas de metadatos antes de la tabla, y el celda_id
se toma del nombre del archivo:
  - "<celda_id>.csv", por ejemplo -36.875_-72.375.csv (nombre que usa descargar_clima.py)
  - "open-meteo-36.87S72.36W96m.csv" (nombre por defecto de Open-Meteo). Como Open-Meteo
    ajusta la coordenada a su propia grilla, se recalcula la celda con la fórmula del contrato.

Ejecutar (PowerShell, desde la carpeta del repo):

  # 1) Lectura de los CSV crudos -> clima_horario y clima_diario
  docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/proyecto/spark/clima_diario.py

  # 2) Prueba del resumen diario con la muestra y comparación con data/sample/clima_diario.csv
  docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/proyecto/spark/clima_diario.py --muestra

  # Revisar lo guardado
  docker exec namenode hdfs dfs -ls /prueba

Para la ejecución final, Ramón cambia las rutas de abajo a /raw/open-meteo/ y /clean/.
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Rutas y parámetros (se cambian aquí para la ejecución final)
# ---------------------------------------------------------------------------
ENTRADA_CRUDO = "hdfs://namenode:9000/raw/open-meteo/"          # carpeta de CSV de Open-Meteo
MUESTRA_HORARIO = "file:///opt/proyecto/data/sample/clima_horario.csv"
ESPERADO_DIARIO = "file:///opt/proyecto/data/sample/clima_diario.csv"  # solo para --muestra
SALIDA_HORARIO = "hdfs://namenode:9000/clean/clima_horario/"
SALIDA_DIARIO = "hdfs://namenode:9000/clean/clima_diario/"

UMBRAL_TEMP = 30.0     # °C, la regla exige temp > 30
UMBRAL_HR = 30.0       # %, la regla exige hr < 30
UMBRAL_VIENTO = 30.0   # km/h, la regla exige viento (o ráfaga) > 30
TOLERANCIA = 0.01      # diferencia máxima aceptada al comparar con la muestra

# Columnas de Open-Meteo -> nombres del contrato
COLUMNAS = {
    "temperature_2m": "temp",
    "relative_humidity_2m": "hr",
    "wind_speed_10m": "viento",
    "wind_gusts_10m": "rafaga",
    "precipitation": "lluvia_hora",
}


def celda(col):
    """Centro de la celda de 0,25° según el contrato: floor(v / 0.25) * 0.25 + 0.125."""
    return F.floor(col / 0.25) * 0.25 + 0.125


def leer_open_meteo(spark, carpeta):
    """Lee los CSV crudos de Open-Meteo y devuelve clima_horario con las columnas del contrato."""
    lineas = (spark.read.text(carpeta)
              .withColumn("archivo", F.regexp_extract(F.input_file_name(), r"([^/]+)\.csv$", 1)))

    # El encabezado de la tabla es la línea que parte con "time,". Se lee una vez para saber
    # en qué posición viene cada variable, así no depende del orden de la URL.
    encabezados = [r.value for r in lineas.filter(F.col("value").startswith("time,"))
                   .select("value").distinct().collect()]
    if not encabezados:
        sys.exit(f"No encontré CSV de Open-Meteo en {carpeta}")
    if len(encabezados) > 1:
        sys.exit(f"Los archivos traen encabezados distintos: {encabezados}")
    nombres = [c.split(" (")[0] for c in encabezados[0].split(",")]  # "temperature_2m (°C)" -> "temperature_2m"
    faltan = [c for c in COLUMNAS if c not in nombres]
    if faltan:
        sys.exit(f"Faltan variables en los CSV: {faltan}. Encabezado: {encabezados[0]}")

    # Solo las filas de datos (empiezan con AAAA-MM-DDTHH:MM). Así se saltan las 3 líneas
    # de metadatos y el encabezado de cada archivo, aunque Spark lea varios archivos juntos.
    datos = lineas.filter(F.col("value").rlike(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2},"))
    partes = F.split("value", ",")

    # celda_id desde el nombre del archivo
    directo = F.regexp_extract("archivo", r"^(-?\d+\.\d{3}_-?\d+\.\d{3})$", 1)
    patron = r"^open-meteo-(\d+(?:\.\d+)?)([NS])(\d+(?:\.\d+)?)([EW])"
    lat = F.regexp_extract("archivo", patron, 1).cast("double") * \
        F.when(F.regexp_extract("archivo", patron, 2) == "S", -1).otherwise(1)
    lon = F.regexp_extract("archivo", patron, 3).cast("double") * \
        F.when(F.regexp_extract("archivo", patron, 4) == "W", -1).otherwise(1)
    desde_coord = F.when(F.regexp_extract("archivo", patron, 1) != "",
                         F.format_string("%.3f_%.3f", celda(lat), celda(lon)))
    celda_id = F.when(directo != "", directo).otherwise(desde_coord)

    tiempo = partes.getItem(nombres.index("time"))
    horario = datos.select(
        F.to_date(F.substring(tiempo, 1, 10)).alias("fecha"),
        F.substring(tiempo, 12, 2).cast("int").alias("hora"),
        celda_id.alias("celda_id"),
        *[F.when(partes.getItem(nombres.index(om)) != "", partes.getItem(nombres.index(om)))
          .cast("double").alias(nuevo) for om, nuevo in COLUMNAS.items()],
        F.col("archivo"),
    )

    sin_celda = [r.archivo for r in horario.filter(F.col("celda_id").isNull())
                 .select("archivo").distinct().collect()]
    if sin_celda:
        sys.exit(f"No pude sacar el celda_id del nombre de estos archivos: {sin_celda}")
    return horario.drop("archivo")


def leer_muestra(spark, ruta):
    """Lee data/sample/clima_horario.csv, que ya viene con las columnas del contrato."""
    return spark.read.csv(ruta, header=True, schema=(
        "fecha DATE, hora INT, celda_id STRING, temp DOUBLE, hr DOUBLE, "
        "viento DOUBLE, rafaga DOUBLE, lluvia_hora DOUBLE"))


def resumir_diario(horario):
    """clima_horario -> clima_diario. Una fila por celda y fecha local."""
    calor_seco = (F.col("temp") > UMBRAL_TEMP) & (F.col("hr") < UMBRAL_HR)
    regla = calor_seco & (F.col("viento") > UMBRAL_VIENTO)
    regla_rafaga = calor_seco & (F.col("rafaga") > UMBRAL_VIENTO)
    return (horario.groupBy("fecha", "celda_id").agg(
        F.max("temp").alias("tmax"),
        F.min("hr").alias("hr_min"),
        F.max("viento").alias("viento_max"),
        F.max("rafaga").alias("rafaga_max"),
        F.round(F.sum("lluvia_hora"), 2).alias("lluvia_dia"),
        # Horas del día en que las tres condiciones se cumplen a la vez (regla simultánea).
        # Si falta un dato en esa hora, la hora no cuenta.
        F.sum(F.when(regla, 1).otherwise(0)).cast("int").alias("horas_regla"),
        F.sum(F.when(regla_rafaga, 1).otherwise(0)).cast("int").alias("horas_regla_rafaga"),
    ))


def comparar(spark, diario, ruta_esperado):
    """Compara la salida con el CSV esperado: mismas filas y diferencias de 0,01 o menos."""
    esperado = spark.read.csv(ruta_esperado, header=True, inferSchema=True) \
        .withColumn("fecha", F.to_date("fecha"))
    columnas = [c for c in esperado.columns if c not in ("fecha", "celda_id")]
    unido = diario.alias("a").join(esperado.alias("e"), ["fecha", "celda_id"], "full_outer")
    faltan = unido.filter(F.col("a.tmax").isNull() | F.col("e.tmax").isNull()).count()
    difs = unido.select(*[F.max(F.abs(F.col(f"a.{c}") - F.col(f"e.{c}"))).alias(c) for c in columnas]).first()
    malas = {c: difs[c] for c in columnas if difs[c] is not None and difs[c] > TOLERANCIA}
    print(f"Filas: salida {diario.count()} | esperado {esperado.count()} | sin pareja {faltan}")
    print("Diferencia máxima por columna:", {c: round(difs[c] or 0, 4) for c in columnas})
    print("RESULTADO:", "COINCIDE con la muestra" if faltan == 0 and not malas else f"NO coincide {malas}")


def main():
    muestra = "--muestra" in sys.argv
    spark = SparkSession.builder.appName("clima-diario").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "America/Santiago")

    horario = leer_muestra(spark, MUESTRA_HORARIO) if muestra else leer_open_meteo(spark, ENTRADA_CRUDO)
    horario = horario.cache()
    horario.write.mode("overwrite").parquet(SALIDA_HORARIO)
    print(f"clima_horario: {horario.count()} filas -> {SALIDA_HORARIO}")
    horario.orderBy("celda_id", "fecha", "hora").show(5)

    diario = resumir_diario(horario).cache()
    diario.write.mode("overwrite").parquet(SALIDA_DIARIO)
    print(f"clima_diario: {diario.count()} filas -> {SALIDA_DIARIO}")
    diario.orderBy("celda_id", "fecha").show(10)

    if muestra:
        comparar(spark, diario, ESPERADO_DIARIO)
    spark.stop()


if __name__ == "__main__":
    main()
