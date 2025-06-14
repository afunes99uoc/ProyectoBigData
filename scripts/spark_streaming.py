from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType
import os

os.environ["HADOOP_HOME"] = "C:/hadoop"

# Crear sesión Spark con el conector Kafka
spark = SparkSession.builder \
    .appName("KafkaSparkStreamingClima") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()

# Esquema del JSON enviado por Kafka
schema = StructType() \
    .add("hora", StringType()) \
    .add("temperatura", DoubleType()) \
    .add("humedad", DoubleType())

# Leer datos desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "clima-barcelona") \
    .option("startingOffsets", "earliest") \
    .load()

# Parsear el mensaje JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Mostrar por consola (debug)
query = json_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
