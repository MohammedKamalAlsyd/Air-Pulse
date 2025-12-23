import os
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    FloatType,
    IntegerType,
    BooleanType,
)

# -----------------------
# Environment / Secrets
# -----------------------
configuration = {
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "S3_BUCKET_NAME": os.getenv("S3_BUCKET_NAME", "spark-streaming-bucket"),
}

# -----------------------
# Topics (default names used by producers)
# -----------------------
TOPICS = {
    "measurement_topic": "sensor.measurements",
    "health_topic": "sensor.health",
    "alarm_topic": "alerts.fire",
    "weather_context_topic": "weather.context",
    "wind_topic": "weather.wind",
    "air_quality_topic": "air.quality",
}

# -----------------------
# Reusable nested schemas
# -----------------------

# Simple geolocation struct used in several payloads
location_schema = StructType(
    [
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
    ]
)

# -----------------------
# Payload schemas
# -----------------------

# Sensor measurement payload produced by generate_sensor_date(...)
# Fields:
#  - id: UUID string for the event
#  - sensor_id: sensor identifier
#  - location: nested struct { latitude, longitude }
#  - timestamp: ISO8601 string
#  - pm25, co2, no2, temperature, humidity, noise_levels, uv_index: numeric readings
sensor_measurement_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("sensor_id", StringType(), True),
        StructField("location", location_schema, True),
        StructField("timestamp", StringType(), True),
        StructField("pm25", DoubleType(), True),         # µg/m³
        StructField("co2", DoubleType(), True),          # ppm
        StructField("no2", DoubleType(), True),          # index / ppb
        StructField("temperature", DoubleType(), True),  # °C
        StructField("humidity", DoubleType(), True),     # %
        StructField("noise_levels", DoubleType(), True), # dB
        StructField("uv_index", DoubleType(), True),     # UV index
    ]
)

# Sensor health payload produced by generate_sensor_health(...)
# Fields:
#  - sensor_id, timestamp
#  - battery_level (percent), signal_strength (dBm)
#  - status (OK/DEGRADED/OFFLINE), sensor_type, model, manufacturer
sensor_health_schema = StructType(
    [
        StructField("sensor_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("battery_level", IntegerType(), True),    # %
        StructField("signal_strength", IntegerType(), True),  # dBm (approx int)
        StructField("status", StringType(), True),            # OK, DEGRADED, OFFLINE
        StructField("sensor_type", StringType(), True),
        StructField("model", StringType(), True),
        StructField("manufacturer", StringType(), True),
    ]
)

# Fire alarm payload produced by generate_fire_alarm(...)
# Fields:
#  - event_id (uuid), sensor_id, timestamp, alarm_type, severity, location, acknowledged
fire_alarm_schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("sensor_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("alarm_type", StringType(), True),   # e.g. "FIRE"
        StructField("severity", StringType(), True),     # LOW/MEDIUM/HIGH/CRITICAL
        StructField("location", location_schema, True),
        StructField("acknowledged", BooleanType(), True),
    ]
)

# City-level air quality / AQI payload produced by generate_air_quality(...)
# Fields:
#  - sensor_id (or city-AQ), timestamp, aqi (int), category (Good/Moderate/...)
air_quality_schema = StructType(
    [
        StructField("sensor_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("aqi", IntegerType(), True),
        StructField("category", StringType(), True),
    ]
)

# Weather context payload produced by generate_weather_context(...)
# Fields:
#  - timestamp, city, temperature (°C), humidity (%), pressure (hPa)
weather_context_schema = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField("city", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("pressure", IntegerType(), True),
    ]
)

# Wind payload produced by generate_wind_data(...)
# Fields:
#  - timestamp, city, wind_speed (m/s or chosen unit), wind_direction (cardinal), wind_direction_degrees
wind_data_schema = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField("city", StringType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("wind_direction", StringType(), True),         # "N", "NE", etc.
        StructField("wind_direction_degrees", DoubleType(), True), # 0-360 float
    ]
)