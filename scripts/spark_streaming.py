from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType
import os

# Configuración de entorno Hadoop
os.environ["HADOOP_HOME"] = "C:/hadoop"

spark = SparkSession.builder \
    .appName("StreamingClimaBarcelona") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("ciudad", StringType()) \
    .add("temperatura_2m", FloatType()) \
    .add("wind_speed_10m", FloatType()) \
    .add("relative_humidity_2m", FloatType()) \
    .add("precipitation", FloatType()) \
    .add("surface_pressure", FloatType())

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "clima-barcelona") \
    .option("startingOffsets", "latest") \
    .load()

df_stream = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

query = df_stream.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", "./checkpoints/clima") \
    .option("path", "./data/oro/clima") \
    .start()

query.awaitTermination()
