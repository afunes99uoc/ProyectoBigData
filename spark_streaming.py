from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

spark = SparkSession.builder \
    .appName("ClimaStreaming") \
    .getOrCreate()

schema = StructType() \
    .add("hora", StringType()) \
    .add("temperatura", DoubleType()) \
    .add("humedad", DoubleType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "clima-barcelona") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

query = json_df.writeStream \
    .format("parquet") \
    .option("path", "data/plata/clima_stream/") \
    .option("checkpointLocation", "data/checkpoint/") \
    .start()

query.awaitTermination()
