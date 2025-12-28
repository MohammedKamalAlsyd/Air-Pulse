import sys
import os
import logging
import traceback
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
    sensor_health_schema,
)

# ---------------------------------------------------------
# Configuration Constants
# ---------------------------------------------------------
S3_BUCKET_NAME = configuration.get("S3_BUCKET_NAME", "spark-streaming-bucket")
S3_BASE_PATH = f"s3a://{S3_BUCKET_NAME}"

# Kafka Broker: 'broker:29092' for inside Docker network, 'localhost:9092' for external
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")

# ---------------------------------------------------------
# Logging
# ---------------------------------------------------------
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("AirPulseStreamingJob")


class StreamManager:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_kafka_stream(self, topic: str, schema: StructType):
        """
        Reads a stream from Kafka and parses the JSON value.
        Returns a streaming DataFrame on success, or None on failure.
        """
        try:
            logger.info("Creating readStream for topic=%s", topic)
            df = (
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

            # Defensive check
            if not getattr(df, "isStreaming", False):
                # Should never happen for readStream, but be defensive
                logger.error("Read DataFrame for topic=%s is not a streaming DataFrame", topic)
                return None

            logger.info("Successfully created streaming DataFrame for topic=%s", topic)
            return df

        except Exception as exc:
            logger.error("Exception while creating read stream for topic=%s: %s", topic, exc)
            logger.debug(traceback.format_exc())
            return None

    def write_stream_to_s3(self, df, stream_name: str):
        """
        Writes the stream to S3 in Parquet format with checkpointing.
        Returns the started StreamingQuery on success, or None on failure.
        """
        if df is None:
            logger.error("write_stream_to_s3 called with df=None for stream %s", stream_name)
            return None

        # Verify it's a streaming DF to avoid the readStream vs read mistake
        if not getattr(df, "isStreaming", False):
            logger.error("DataFrame provided for writing (%s) is not streaming. Aborting write.", stream_name)
            return None

        checkpoint_path = f"{S3_BASE_PATH}/checkpoints/{stream_name}"
        output_path = f"{S3_BASE_PATH}/data/{stream_name}"

        try:
            logger.info("Starting writeStream for %s -> %s (checkpoint: %s)", stream_name, output_path, checkpoint_path)
            query = (
                df.writeStream.format("parquet")
                .option("checkpointLocation", checkpoint_path)
                .option("path", output_path)
                .outputMode("append")
                .queryName(stream_name)
                .trigger(processingTime="10 seconds")  # Configurable trigger
                .start()
            )

            logger.info("Started query %s (id=%s)", stream_name, query.id)
            return query

        except Exception as exc:
            logger.error("Failed to start writeStream for %s: %s", stream_name, exc)
            logger.debug(traceback.format_exc())
            return None


def main():
    # ---------------------------------------------------------
    # Spark Session Initialization
    # ---------------------------------------------------------
    spark = (
        SparkSession.builder.appName("AirPulseStreamingJob")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", configuration.get("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    manager = StreamManager(spark)

    # ---------------------------------------------------------
    # Stream Definitions
    # ---------------------------------------------------------
    streams_config = [
        {"topic": TOPICS["measurement_topic"], "schema": sensor_measurement_schema, "name": "sensor_measurements"},
        {"topic": TOPICS["health_topic"], "schema": sensor_health_schema, "name": "sensor_health"},
        {"topic": TOPICS["air_quality_topic"], "schema": air_quality_schema, "name": "air_quality"},
        {"topic": TOPICS["weather_context_topic"], "schema": weather_context_schema, "name": "weather_context"},
        {"topic": TOPICS["wind_topic"], "schema": wind_data_schema, "name": "wind_data"},
    ]

    # ---------------------------------------------------------
    # Execution Loop
    # ---------------------------------------------------------
    active_queries = []

    logger.info("Starting streams... Writing to %s", S3_BASE_PATH)

    for cfg in streams_config:
        topic = cfg["topic"]
        name = cfg["name"]
        schema = cfg["schema"]

        logger.info("Initializing stream for: %s (Topic: %s)", name, topic)

        # 1. Read (wrapped with try/except inside manager.read_kafka_stream)
        raw_df = manager.read_kafka_stream(topic, schema)
        if raw_df is None:
            logger.error("Skipping stream %s because read_kafka_stream failed.", name)
            continue

        # 2. Transform (Optional: Add specific processing logic here if needed)
        # Example: processed_df = raw_df.withColumn("ingestion_time", current_timestamp())
        processed_df = raw_df  # placeholder for any future transformations
        
        processed_df.show(2)
        print('prinitng data here')

        # 3. Write (manager.write_stream_to_s3 handles exceptions and returns None on failure)
        query = manager.write_stream_to_s3(processed_df, name)
        if query is None:
            logger.error("Failed to start write stream for %s; check logs.", name)
            continue

        active_queries.append(query)

    # ---------------------------------------------------------
    # Await Termination
    # ---------------------------------------------------------
    if active_queries:
        try:
            logger.info("Awaiting termination of any query...")
            spark.streams.awaitAnyTermination()
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received: attempting to stop active streams gracefully...")
            for q in active_queries:
                try:
                    logger.info("Stopping query id=%s, name=%s", q.id, q.name)
                    q.stop()
                except Exception:
                    logger.debug(traceback.format_exc())
            logger.info("Stopped all queries.")
        except Exception as exc:
            logger.error("Error while awaiting termination: %s", exc)
            logger.debug(traceback.format_exc())
    else:
        logger.warning("No queries were started. Exiting.")


if __name__ == "__main__":
    main()
