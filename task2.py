# task2.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as F_sum, avg as F_avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

HOST = "127.0.0.1"
PORT = 9999

OUTPUT_DIR = "/workspaces/Handson-L8-Spark-SQL_Streaming/output/task2/"
CHECKPOINT_DIR = "/workspaces/Handson-L8-Spark-SQL_Streaming/checkpoints/task2/"

# 1) Spark session
spark = (
    SparkSession.builder
    .appName("RideSharingAnalytics-Task2")
    .getOrCreate()
)

# 2) Schema matching your generator
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# 3) Read from socket
lines = (
    spark.readStream
         .format("socket")
         .option("host", HOST)
         .option("port", PORT)
         .load()
)

# 4) Parse JSON -> columns
rides = (
    lines.select(from_json(col("value"), schema).alias("j"))
         .select("j.*")
         .where(col("trip_id").isNotNull())
)

# 5) Real-time aggregations per driver
agg_by_driver = (
    rides.groupBy("driver_id")
         .agg(
             F_sum("fare_amount").alias("total_fare"),
             F_avg("distance_km").alias("avg_distance")
         )
)

# 6) Write each micro-batch to CSV files under output/task2/
def write_batch(df, epoch_id: int):
    # one folder per epoch (avoids conflicts; keeps history)
    (
        df.coalesce(1)  # optional: fewer files per epoch
          .write
          .mode("overwrite")  # overwrite this epoch's folder if retried
          .option("header", "true")
          .csv(f"{OUTPUT_DIR}epoch={epoch_id}")
    )

query = (
    agg_by_driver.writeStream
                 .foreachBatch(write_batch)
                 .option("checkpointLocation", CHECKPOINT_DIR)
                 .outputMode("update")  # update table per trigger, foreachBatch handles file writes
                 .start()
)

query.awaitTermination()