from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, StringType, FloatType
import os

# Configuración de entorno Hadoop
os.environ["HADOOP_HOME"] = "C:/hadoop"

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("StreamingClimaBarcelona") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Definir esquema de los datos del mensaje
schema = StructType() \
    .add("ciudad", StringType()) \
    .add("temperatura_2m", FloatType()) \
    .add("wind_speed_10m", FloatType()) \
    .add("relative_humidity_2m", FloatType()) \
    .add("precipitation", FloatType()) \
    .add("surface_pressure", FloatType())

# Leer de Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "clima-barcelona") \
    .option("startingOffsets", "latest") \
    .load()

# Parsear JSON
df_stream = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Cálculo de KPIs por barrio (agrupación)
kpis = df_stream.groupBy("ciudad").agg(
    avg("temperatura_2m").alias("temp_media"),
    avg("wind_speed_10m").alias("viento_medio"),
    avg("relative_humidity_2m").alias("humedad_media"),
    avg("precipitation").alias("precipitacion_media"),
    avg("surface_pressure").alias("presion_media")
)

# Escritura en carpeta oro
query = kpis.writeStream \
    .outputMode("complete") \
    .format("parquet") \
    .option("checkpointLocation", "./checkpoints/clima") \
    .option("path", "./data/oro/clima") \
    .start()

query.awaitTermination()
