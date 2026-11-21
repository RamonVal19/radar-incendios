"""Prueba de que Spark lee desde HDFS. No limpia nada: solo cuenta filas.

Ejecutar:
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/proyecto/spark/prueba_hdfs.py
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("prueba-hdfs").getOrCreate()

focos = spark.read.csv("hdfs://namenode:9000/raw/firms/", header=True)
print("Focos leidos desde HDFS:", focos.count())
focos.groupBy("type").count().orderBy("type").show()

spark.stop()
