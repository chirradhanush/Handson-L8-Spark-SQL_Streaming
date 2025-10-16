# task3.py
# --- imports ---
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, sum as F_sum
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

HOST = "127.0.0.1"
PORT = 9999

OUTPUT_DIR = "/workspaces/Handson-L8-Spark-SQL_Streaming/output/task3/"
CHECKPOINT_DIR = "/workspaces/Handson-L8-Spark-SQL_Streaming/checkpoints/task3/"

# 1) Create Spark session
spark = (
    SparkSession.builder
    .appName("RideSharingAnalytics-Task3")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 2) Schema for incoming JSON (matches your generator)
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True),  # "YYYY-MM-DD HH:mm:ss"
])

# 3) Read streaming data from socket
lines = (
    spark.readStream
         .format("socket")
         .option("host", HOST)
         .option("port", PORT)
         .load()
)

# 4) Parse JSON to columns
rides = (
    lines.select(from_json(col("value"), schema).alias("j"))
         .select("j.*")
         .where(col("trip_id").isNotNull())
)

# 5) Convert timestamp -> TimestampType & add watermark
#    Watermark = 10 minutes (tune as needed for your late-data policy)
rides_ts = (
    rides.withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
         .withWatermark("event_time", "10 minutes")
)

# 6) Windowed aggregation: 5-minute window, sliding every 1 minute
#    Sum of fare_amount within each window
from pyspark.sql.functions import window

windowed = (
    rides_ts
    .groupBy(window(col("event_time"), "5 minutes", "1 minute"))
    .agg(F_sum("fare_amount").alias("total_fare"))
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_fare")
    )
)

# 7) Writer for each micro-batch -> CSV with header
def write_batch(df, epoch_id: int):
    (
        df.coalesce(1)           # optional: one file per epoch
          .write
          .mode("overwrite")     # safe on retries of the same epoch
          .option("header", "true")
          .csv(f"{OUTPUT_DIR}epoch={epoch_id}")
    )

# 8) Start the stream with foreachBatch
query = (
    windowed.writeStream
            .foreachBatch(write_batch)
            .option("checkpointLocation", CHECKPOINT_DIR)
            .outputMode("update")   # update works with aggregations + watermark
            .start()
)

# 9) Keep running
query.awaitTermination()