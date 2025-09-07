
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "redditsindhu") \
    .load()

# Perform any necessary transformations or processing
# For example, converting binary key and value to string
messages_df = df.selectExpr("CAST(value AS STRING)")

# Write data to a Hive table
query = df \
     .writeStream \
     .outputMode("append") \
     .format("parquet") \
     .option("checkpointLocation", "gs://sindhun/Backup") \
     .option("path", "gs://sindhun/reddit/") \
     .start()


# Await termination
query.awaitTermination()

