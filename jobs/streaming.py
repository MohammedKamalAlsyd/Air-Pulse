from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from config import configuration, TOPICS, sensor_measurement_schema, air_quality_schema, weather_context_schema, wind_data_schema, sensor_health_schema

def read_stream_from_kafka(spark, topic, schema):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "broker:29092")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .select(from_json("value", schema).alias("data"), "key")
        .withWatermark("data.timestamp", "10 minutes")
    )
    
def write_stream_to_s3(df, checkpoint_location, query_name, output, output_mode="append"):
    return (
        df.writeStream.format("parquet")
        .option("checkpointLocation", checkpoint_location)
        .option("path", output)
        .outputMode(output_mode)
        .queryName(query_name)
        .start()
    )

def main():
    spark = SparkSession.builder.appName("AirPulseJob")\
        .config(
            "spark.jars.packages",
            ','.join([
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0",
                "org.apache.hadoop:hadoop-aws:3.4.2",
                "com.amazonaws:aws-java-sdk-bundle:1.12.796"
            ])
        )\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.access.key",configuration["AWS_ACCESS_KEY_ID"])\
        .config("spark.hadoop.fs.s3a.secret.key",configuration["AWS_SECRET_ACCESS_KEY"])\
        .config("spark.hadoop.fs.s3a.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    
    # Reading The Data Stream from Kafka
    sensor_measurements_df = read_stream_from_kafka(
        spark, TOPICS["measurement_topic"], sensor_measurement_schema
    ).alias("sensor_measurements")
    sensor_health_df = read_stream_from_kafka(
        spark, TOPICS["health_topic"], sensor_health_schema
    ).alias("sensor_health")
    air_quality_df = read_stream_from_kafka(
        spark, TOPICS["air_quality_topic"], air_quality_schema
    ).alias("air_quality")
    weather_context_df = read_stream_from_kafka(
        spark, TOPICS["weather_context_topic"], weather_context_schema
    ).alias("weather_context")
    wind_data_df = read_stream_from_kafka(
        spark, TOPICS["wind_topic"], wind_data_schema
    ).alias("wind_data")
    
    # apply any transformations or joins as needed here
    
    
    # write the stream into s3
    query1 = write_stream_to_s3(
        sensor_measurements_df, "s3a://spark-streaming-bucket/checkpoints/sensor_data", "sensor_measurements", "s3a://spark-streaming-bucket/data/sensor_data")
    query2 = write_stream_to_s3(
        sensor_health_df, "s3a://spark-streaming-bucket/checkpoints/sensor_health", "sensor_health", "s3a://spark-streaming-bucket/data/sensor_health")
    query3 = write_stream_to_s3(
        air_quality_df, "s3a://spark-streaming-bucket/checkpoints/air_quality", "air_quality", "s3a://spark-streaming-bucket/data/air_quality")
    query4 = write_stream_to_s3(
        weather_context_df, "s3a://spark-streaming-bucket/checkpoints/weather_context", "weather_context", "s3a://spark-streaming-bucket/data/weather_context")
    query5 = write_stream_to_s3(
        wind_data_df, "s3a://spark-streaming-bucket/checkpoints/wind_data", "wind_data", "s3a://spark-streaming-bucket/data/wind_data")
    
    query5.awaitTermination()
    
if __name__ == "__main__":
    main()