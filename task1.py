# task1.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

HOST = "127.0.0.1"
PORT = 9999

# 1️⃣ Create Spark session
spark = (
    SparkSession.builder
    .appName("RideSharingAnalytics-Task1")
    .getOrCreate()
)

# 2️⃣ Define schema (matching data_generator)
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# 3️⃣ Read from socket stream
lines = (
    spark.readStream
         .format("socket")
         .option("host", HOST)
         .option("port", PORT)
         .load()
)

# 4️⃣ Parse JSON string into columns
parsed = (
    lines
    .select(from_json(col("value"), schema).alias("j"))
    .select("j.*")
    .where(col("trip_id").isNotNull())
)

# 5️⃣ Write to CSV files under output/task1/
query = (
    parsed.writeStream
          .format("csv")
          .option("path", "/workspaces/Handson-L8-Spark-SQL_Streaming/output/task1/")
          .option("checkpointLocation", "/workspaces/Handson-L8-Spark-SQL_Streaming/checkpoints/task1/")
          .option("header", "true")
          .outputMode("append")
          .start()
)

# 6️⃣ Keep running
query.awaitTermination()