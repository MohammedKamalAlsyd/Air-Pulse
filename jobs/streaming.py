import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType

# Import configurations and schemas from your config.py
from config import (
    configuration, 
    TOPICS, 
    sensor_measurement_schema, 
    air_quality_schema, 
    weather_context_schema, 
    wind_data_schema, 
    sensor_health_schema
)

# ---------------------------------------------------------
# Configuration Constants
# ---------------------------------------------------------
# Define the S3 Bucket Name here or add it to config.py
# If running via Docker/CloudFormation, ensure this matches your created bucket.
S3_BUCKET_NAME = configuration.get("S3_BUCKET_NAME", "spark-streaming-bucket")
S3_BASE_PATH = f"s3a://{S3_BUCKET_NAME}"

# Kafka Broker: 'broker:29092' for inside Docker network, 'localhost:9092' for external
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")

class StreamManager:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_kafka_stream(self, topic: str, schema: StructType):
        """Reads a stream from Kafka and parses the JSON value."""
        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
            .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
            .select(from_json(col("value"), schema).alias("data"), col("key"))
            .select("data.*", "key")  # Flatten struct for easier writing
        )

    def write_stream_to_s3(self, df, stream_name: str):
        """Writes the stream to S3 in Parquet format with checkpointing."""
        checkpoint_path = f"{S3_BASE_PATH}/checkpoints/{stream_name}"
        output_path = f"{S3_BASE_PATH}/data/{stream_name}"

        return (
            df.writeStream.format("parquet")
            .option("checkpointLocation", checkpoint_path)
            .option("path", output_path)
            .outputMode("append")
            .queryName(stream_name)
            .trigger(processingTime="10 seconds")  # Configurable trigger
            .start()
        )

def main():
    # ---------------------------------------------------------
    # Spark Session Initialization
    # ---------------------------------------------------------
    spark = SparkSession.builder.appName("AirPulseStreamingJob")\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.access.key", configuration.get("AWS_ACCESS_KEY_ID"))\
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get("AWS_SECRET_ACCESS_KEY"))\
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    manager = StreamManager(spark)

    # ---------------------------------------------------------
    # Stream Definitions
    # ---------------------------------------------------------
    # Map topics to their schemas and the desired folder name in S3
    streams_config = [
        {
            "topic": TOPICS["measurement_topic"],
            "schema": sensor_measurement_schema,
            "name": "sensor_measurements"
        },
        {
            "topic": TOPICS["health_topic"],
            "schema": sensor_health_schema,
            "name": "sensor_health"
        },
        {
            "topic": TOPICS["air_quality_topic"],
            "schema": air_quality_schema,
            "name": "air_quality"
        },
        {
            "topic": TOPICS["weather_context_topic"],
            "schema": weather_context_schema,
            "name": "weather_context"
        },
        {
            "topic": TOPICS["wind_topic"],
            "schema": wind_data_schema,
            "name": "wind_data"
        }
    ]

    # ---------------------------------------------------------
    # Execution Loop
    # ---------------------------------------------------------
    active_queries = []

    print(f"Starting streams... Writing to {S3_BASE_PATH}")

    for config in streams_config:
        try:
            print(f"Initializing stream for: {config['name']} (Topic: {config['topic']})")
            
            # 1. Read
            raw_df = manager.read_kafka_stream(config["topic"], config["schema"])
            
            # 2. Transform (Optional: Add specific processing logic here if needed)
            # Example: processed_df = raw_df.withColumn("ingestion_time", current_timestamp())
            
            # 3. Write
            query = manager.write_stream_to_s3(raw_df, config["name"])
            active_queries.append(query)
            
        except Exception as e:
            print(f"Failed to start stream for {config['name']}: {e}")

    # ---------------------------------------------------------
    # Await Termination
    # ---------------------------------------------------------
    # Wait for any of the queries to terminate (due to error or stop)
    if active_queries:
        spark.streams.awaitAnyTermination()
    else:
        print("No queries were started.")

if __name__ == "__main__":
    main()