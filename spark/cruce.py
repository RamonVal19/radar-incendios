"""Job de Spark del rol C: cruza focos, clima diario y celdas, y evalúa la regla 30-30-30.

Responsable: Alan Caro. Contrato: schemas/esquemas.md, secciones 5 (resultado) y 6 (evaluacion_regla).

Qué hace:
  1. Arma una fila por celda y día (los días salen de clima_diario), haya o no focos,
     con n_focos, frp_total, anio, temporada (julio a junio) y es_validacion.
  2. Con una ventana por celda ordenada por fecha calcula las columnas del día anterior
     (lag) y dias_sin_lluvia. El primer día de cada celda deja las columnas _d1 vacías y
     dias_sin_lluvia parte contando desde ese día.
  3. Calcula las cuatro versiones de la regla (diaria, simultanea, rafaga y diaria_d1)
     y dia_muchos_focos (n_focos >= UMBRAL_MUCHOS_FOCOS).
  4. Calcula evaluacion_regla por versión y temporada. Los días con 1 a UMBRAL-1 focos no
     cuentan ni como acierto ni como falsa alarma. Si horas_regla viene vacía (plan B),
     regla_simultanea y regla_rafaga quedan vacías y esas versiones no se evalúan.
  5. Guarda resultado en Parquet particionada por anio y zona, y evaluacion_regla en Parquet.

Ejecutar (desde la carpeta del repo):

  # Con la muestra (CSV de data/sample/), guarda en /prueba/
  docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \\
      /opt/proyecto/spark/cruce.py

  # Prueba contra la salida esperada de la muestra (generada con umbral 3)
  docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \\
      /opt/proyecto/spark/cruce.py --umbral 3 --comparar

  # Datos reales (Parquet en /clean/)
  docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \\
      /opt/proyecto/spark/cruce.py --real

  # Revisar lo guardado
  docker exec namenode hdfs dfs -ls /prueba
"""
import argparse
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Parámetros (se cambian aquí o con las opciones de la línea de comandos)
# ---------------------------------------------------------------------------
UMBRAL_MUCHOS_FOCOS = 8   # percentil 90 de n_focos (días con focos, temporadas 2013-2024), ver data/umbral.txt
TEMPORADA_VALIDACION = 2025
TOLERANCIA = 0.01         # diferencia máxima aceptada al comparar con la muestra

# Muestra (CSV)
MUESTRA = {
    "focos": "file:///opt/proyecto/data/sample/focos.csv",
    "clima": "file:///opt/proyecto/data/sample/clima_diario.csv",
    "celdas": "file:///opt/proyecto/data/sample/celdas.csv",
    "salida_resultado": "hdfs://namenode:9000/prueba/resultado/",
    "salida_evaluacion": "hdfs://namenode:9000/prueba/evaluacion_regla/",
    "formato": "csv",
}
# Datos reales (Parquet)
REAL = {
    "focos": "hdfs://namenode:9000/clean/focos/",
    "clima": "hdfs://namenode:9000/clean/clima_diario/",
    "celdas": "hdfs://namenode:9000/clean/celdas/",
    "salida_resultado": "hdfs://namenode:9000/clean/resultado/",
    "salida_evaluacion": "hdfs://namenode:9000/clean/evaluacion_regla/",
    "formato": "parquet",
}
ESPERADO_RESULTADO = "file:///opt/proyecto/data/sample/resultado.csv"
ESPERADO_EVALUACION = "file:///opt/proyecto/data/sample/evaluacion_regla.csv"

# Versiones de la regla -> columna de resultado
VERSIONES = {
    "diaria": "regla_diaria",
    "simultanea": "regla_simultanea",
    "rafaga": "regla_rafaga",
    "diaria_d1": "regla_diaria_d1",
}

COLUMNAS_RESULTADO = [
    "fecha", "anio", "temporada", "es_validacion", "celda_id", "lat_celda", "lon_celda", "zona",
    "es_control", "n_focos", "frp_total", "tmax", "hr_min", "viento_max", "rafaga_max", "lluvia_dia",
    "horas_regla", "horas_regla_rafaga", "tmax_d1", "hr_min_d1", "rafaga_max_d1", "dias_sin_lluvia",
    "regla_diaria", "regla_simultanea", "regla_rafaga", "regla_diaria_d1", "dia_muchos_focos",
]


def leer(spark, ruta, formato):
    if formato == "csv":
        return spark.read.csv(ruta, header=True)  # todo como texto, los tipos se fijan abajo
    return spark.read.parquet(ruta)


def leer_entradas(spark, rutas):
    fmt = rutas["formato"]
    celdas = leer(spark, rutas["celdas"], fmt).select(
        "celda_id",
        F.col("lat_celda").cast("double").alias("lat_celda"),
        F.col("lon_celda").cast("double").alias("lon_celda"),
        F.col("zona").cast("int").alias("zona"),
        F.col("es_control").cast("boolean").alias("es_control"),
    )
    clima = leer(spark, rutas["clima"], fmt).select(
        F.col("fecha").cast("date").alias("fecha"),
        "celda_id",
        *[F.col(c).cast("double").alias(c) for c in ("tmax", "hr_min", "viento_max", "rafaga_max", "lluvia_dia")],
        F.col("horas_regla").cast("int").alias("horas_regla"),
        F.col("horas_regla_rafaga").cast("int").alias("horas_regla_rafaga"),
    )
    focos = leer(spark, rutas["focos"], fmt).select(
        F.col("fecha").cast("date").alias("fecha"),
        "celda_id",
        F.col("frp").cast("double").alias("frp"),
    )
    return celdas, clima, focos


def armar_resultado(celdas, clima, focos, umbral):
    focos_dia = focos.groupBy("celda_id", "fecha").agg(
        F.count("*").cast("int").alias("n_focos"),
        F.round(F.sum("frp"), 2).alias("frp_total"),
    )

    # Una fila por celda y día del clima. Solo entran las celdas de la lista fija.
    base = (clima.join(celdas, "celda_id", "inner")
            .join(focos_dia, ["celda_id", "fecha"], "left")
            .fillna({"n_focos": 0, "frp_total": 0.0}))

    w = Window.partitionBy("celda_id").orderBy("fecha")

    # dias_sin_lluvia: cada día con lluvia (>= 1 mm) abre un grupo nuevo, y dentro del grupo se
    # cuentan los días secos acumulados. El día con lluvia vale 0. Si lluvia_dia viene vacía,
    # se toma como día sin dato y reinicia la cuenta (decisión nuestra, no está en el contrato).
    seco = F.coalesce(F.col("lluvia_dia") < 1, F.lit(False))
    base = (base.withColumn("_seco", seco.cast("int"))
            .withColumn("_grupo", F.sum(1 - F.col("_seco")).over(w.rowsBetween(Window.unboundedPreceding, 0))))
    w_grupo = (Window.partitionBy("celda_id", "_grupo").orderBy("fecha")
               .rowsBetween(Window.unboundedPreceding, 0))

    # Las comparaciones con null dan null, así que con plan B (horas_regla vacía)
    # regla_simultanea y regla_rafaga quedan vacías, como pide el contrato.
    regla_diaria = (F.col("tmax") > 30) & (F.col("hr_min") < 30) & (F.col("viento_max") > 30)
    anio = F.year("fecha")
    temporada = F.when(F.month("fecha") >= 7, anio + 1).otherwise(anio)

    resultado = (base
                 .withColumn("dias_sin_lluvia", F.sum("_seco").over(w_grupo).cast("int"))
                 .withColumn("anio", anio)
                 .withColumn("temporada", temporada)
                 .withColumn("es_validacion", F.col("temporada") == TEMPORADA_VALIDACION)
                 .withColumn("tmax_d1", F.lag("tmax").over(w))
                 .withColumn("hr_min_d1", F.lag("hr_min").over(w))
                 .withColumn("rafaga_max_d1", F.lag("rafaga_max").over(w))
                 .withColumn("regla_diaria", regla_diaria)
                 .withColumn("regla_simultanea", F.col("horas_regla") >= 1)
                 .withColumn("regla_rafaga", F.col("horas_regla_rafaga") >= 1)
                 .withColumn("regla_diaria_d1", F.lag("regla_diaria").over(w))
                 .withColumn("dia_muchos_focos", F.col("n_focos") >= umbral)
                 .select(*COLUMNAS_RESULTADO))
    return resultado


def armar_evaluacion(resultado):
    partes = []
    for version, col in VERSIONES.items():
        alerta = F.coalesce(F.col(col), F.lit(False))
        muchos = F.col("dia_muchos_focos")
        partes.append(resultado.groupBy("temporada").agg(
            F.lit(version).alias("version"),
            F.sum(alerta.cast("int")).cast("int").alias("dias_alerta"),
            F.sum((alerta & muchos).cast("int")).cast("int").alias("aciertos"),
            F.sum((alerta & (F.col("n_focos") == 0)).cast("int")).cast("int").alias("falsas_alarmas"),
            F.sum((muchos & ~alerta).cast("int")).cast("int").alias("omisiones"),
            F.sum(F.when(alerta, F.col("n_focos")).otherwise(0)).cast("int").alias("focos_en_alerta"),
            F.sum("n_focos").cast("int").alias("focos_totales"),
            F.round(F.avg(muchos.cast("double")), 4).alias("tasa_base"),
            F.count(F.col(col)).alias("_evaluables"),  # 0 si la regla viene toda vacía (plan B)
        ))
    evaluacion = partes[0]
    for p in partes[1:]:
        evaluacion = evaluacion.unionByName(p)
    return (evaluacion.filter(F.col("_evaluables") > 0)
            .select("version", F.col("temporada").cast("int").alias("temporada"), "dias_alerta", "aciertos",
                    "falsas_alarmas", "omisiones", "focos_en_alerta", "focos_totales", "tasa_base"))


def comparar(spark, obtenido, ruta_esperado, claves):
    """Compara con el CSV esperado: mismas filas y columnas, números con diferencia <= TOLERANCIA."""
    esperado = spark.read.csv(ruta_esperado, header=True, inferSchema=True)
    n_obt, n_esp = obtenido.count(), esperado.count()
    print(f"Filas: obtenidas {n_obt}, esperadas {n_esp}")
    faltan = set(esperado.columns) - set(obtenido.columns)
    if faltan:
        print(f"  Faltan columnas: {sorted(faltan)}")
        return False

    o = obtenido.select(*[F.col(c).alias("o_" + c) for c in esperado.columns])
    e = esperado.select(*[F.col(c).alias("e_" + c) for c in esperado.columns])
    cond = [F.col("o_" + k).cast("string") == F.col("e_" + k).cast("string") for k in claves]
    unido = o.join(e, cond, "full_outer")

    numericos = {f.name for f in esperado.schema.fields
                 if f.dataType.typeName() in ("integer", "long", "double", "float")}
    errores = []
    for c in esperado.columns:
        oc, ec = F.col("o_" + c), F.col("e_" + c)
        if c in numericos:
            distinto = ~((oc.isNull() & ec.isNull()) |
                         (F.abs(oc.cast("double") - ec.cast("double")) <= TOLERANCIA))
        else:
            distinto = ~oc.cast("string").eqNullSafe(ec.cast("string"))
        errores.append(F.sum(F.coalesce(distinto, F.lit(True)).cast("int")).alias(c))
    malos = unido.agg(*errores).first().asDict()
    malos = {c: n for c, n in malos.items() if n}
    if malos or n_obt != n_esp:
        print(f"  NO coincide. Filas distintas por columna: {malos}")
        return False
    print("  Coincide con la muestra")
    return True


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--real", action="store_true", help="usa las rutas de /clean/ y Parquet")
    p.add_argument("--umbral", type=int, default=UMBRAL_MUCHOS_FOCOS,
                   help=f"umbral de dia_muchos_focos (por defecto {UMBRAL_MUCHOS_FOCOS})")
    p.add_argument("--comparar", action="store_true",
                   help="compara con data/sample/resultado.csv y evaluacion_regla.csv (usar con --umbral 3)")
    for k in MUESTRA:
        if k != "formato":
            p.add_argument("--" + k.replace("_", "-"), dest=k, default=None)
    p.add_argument("--esperado-resultado", default=ESPERADO_RESULTADO)
    p.add_argument("--esperado-evaluacion", default=ESPERADO_EVALUACION)
    a = p.parse_args()

    rutas = dict(REAL if a.real else MUESTRA)
    rutas.update({k: getattr(a, k) for k in MUESTRA if k != "formato" and getattr(a, k)})

    spark = SparkSession.builder.appName("cruce").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "America/Santiago")
    if not a.real:
        spark.conf.set("spark.sql.shuffle.partitions", "8")  # la muestra es chica: menos tareas, más rápido

    celdas, clima, focos = leer_entradas(spark, rutas)
    resultado = armar_resultado(celdas, clima, focos, a.umbral).cache()
    evaluacion = armar_evaluacion(resultado).cache()

    # Focos que no entran al cruce (celda fuera de la lista o día sin clima). Debería ser 0.
    perdidos = focos.join(resultado.select("celda_id", "fecha"), ["celda_id", "fecha"], "left_anti").count()
    print(f"Umbral dia_muchos_focos: n_focos >= {a.umbral}")
    print(f"Focos sin fila en resultado (celda fuera de la lista o día sin clima): {perdidos}")

    print(f"resultado: {resultado.count()} filas")
    resultado.orderBy("celda_id", "fecha").show(10)
    print(f"evaluacion_regla: {evaluacion.count()} filas")
    evaluacion.orderBy("temporada", "version").show(50, truncate=False)

    # repartition deja un archivo por carpeta anio=/zona= en vez de muchos archivos chicos
    (resultado.repartition("anio", "zona")
     .write.mode("overwrite").partitionBy("anio", "zona").parquet(rutas["salida_resultado"]))
    evaluacion.write.mode("overwrite").parquet(rutas["salida_evaluacion"])
    print(f"Guardado en {rutas['salida_resultado']} y {rutas['salida_evaluacion']}")

    if a.comparar:
        ok1 = comparar(spark, resultado, a.esperado_resultado, ["celda_id", "fecha"])
        ok2 = comparar(spark, evaluacion, a.esperado_evaluacion, ["version", "temporada"])
        spark.stop()
        sys.exit(0 if ok1 and ok2 else 1)
    spark.stop()


if __name__ == "__main__":
    main()
