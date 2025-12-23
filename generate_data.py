from __future__ import annotations
import os
import time
import threading
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple, List, Optional

from dotenv import load_dotenv

# Producers
from producer.weather_producer import WeatherProducer
from producer.sensor_producer import SensorProducer
from producer.alarm_producer import AlarmProducer

load_dotenv()

# -------------------------
# Environment / topics
# -------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").strip()
USE_KAFKA = bool(KAFKA_BOOTSTRAP_SERVERS)

WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather.context")
WIND_TOPIC = os.getenv("WIND_TOPIC", "weather.wind")
AIR_QUALITY_TOPIC = os.getenv("AIR_QUALITY_TOPIC", "air.quality")

SENSOR_MEASUREMENT_TOPIC = os.getenv("SENSOR_MEASUREMENT_TOPIC", "sensor.measurements")
SENSOR_HEALTH_TOPIC = os.getenv("SENSOR_HEALTH_TOPIC", "sensor.health")

ALARM_TOPIC = os.getenv("ALARM_TOPIC", "alerts.fire")

# -------------------------
# Kafka configuration passed down to BaseProducer
# -------------------------
kafka_conf: Optional[Dict[str, object]]
if USE_KAFKA:
    kafka_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        # Add other confluent_kafka producer configs here as needed
        # e.g. "acks": "all", "linger.ms": 5, etc.
    }
else:
    kafka_conf = None  # BaseProducer will print debug lines instead of sending to Kafka

print("Kafka enabled:", USE_KAFKA)
if USE_KAFKA:
    print("Kafka bootstrap.servers:", KAFKA_BOOTSTRAP_SERVERS)

# -------------------------
# Domain configuration
# -------------------------
# Two sensors with explicit locations
SENSOR_IDS: List[str] = ["sensor-1", "sensor-2"]
SENSOR_LOCATIONS: Dict[str, Tuple[float, float]] = {
    "sensor-1": (30.0444, 31.2357),  # Cairo (example)
    "sensor-2": (30.0500, 31.2333),
}

# One alarm producer configured to watch same sensors (rare events)
ALARM_SENSOR_IDS = SENSOR_IDS

# One weather producer with a couple of cities
CITIES = [
    {"city": "Cairo", "lat": 30.0444, "lon": 31.2357},
    {"city": "Giza", "lat": 30.0131, "lon": 31.2089},
]

# -------------------------
# Timing (seconds)
# -------------------------
SENSOR_INTERVAL_SEC = int(os.getenv("SENSOR_INTERVAL_SEC", "1"))          # sensor measurements every 1s
HEALTH_INTERVAL_SEC = int(os.getenv("HEALTH_INTERVAL_SEC", "5"))        # sensor health every 5s
WEATHER_INTERVAL_SEC = int(os.getenv("WEATHER_INTERVAL_SEC", "5"))      # weather updates every 5s
ALARM_INTERVAL_SEC = int(os.getenv("ALARM_INTERVAL_SEC", "30"))         # alarms checked/produced every 30s

# -------------------------
# Instantiate producers
# -------------------------
weather_producer = WeatherProducer(
    cities=CITIES,
    kafka_conf=kafka_conf,
    weather_topic=WEATHER_TOPIC,
    wind_topic=WIND_TOPIC,
    air_quality_topic=AIR_QUALITY_TOPIC,
)

sensor_producer = SensorProducer(
    sensor_ids=SENSOR_IDS,
    locations=SENSOR_LOCATIONS,
    kafka_conf=kafka_conf,
    measurement_topic=SENSOR_MEASUREMENT_TOPIC,
    health_topic=SENSOR_HEALTH_TOPIC,
)

alarm_producer = AlarmProducer(
    sensor_ids=ALARM_SENSOR_IDS,
    locations=SENSOR_LOCATIONS,
    kafka_conf=kafka_conf,
    topic=ALARM_TOPIC,
)

# -------------------------
# Worker wrappers
# Each worker calls the producer.run() with iterations=1 and then sleeps for the interval.
# This keeps timestamps fresh and gives each producer independent pacing.
# -------------------------
stop_event = threading.Event()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def sensor_worker(producer: SensorProducer, interval_sec: int) -> None:
    """Produces measurements + health for all sensors at the configured cadence."""
    try:
        while not stop_event.is_set():
            ts = _utc_now()
            # produce one 'iteration' (each iteration will produce one measurement+health per sensor)
            producer.run(start_time=ts, delta=timedelta(seconds=interval_sec), iterations=1)
            # sleep for cadence (allow small wakeup to handle shutdown)
            if stop_event.wait(interval_sec):
                break
    except Exception as exc:
        print("sensor_worker error:", exc)


def weather_worker(producer: WeatherProducer, interval_sec: int) -> None:
    """Produces weather, wind, and city air quality at a lower cadence than sensors."""
    try:
        while not stop_event.is_set():
            ts = _utc_now()
            producer.run(start_time=ts, delta=timedelta(seconds=interval_sec), iterations=1)
            if stop_event.wait(interval_sec):
                break
    except Exception as exc:
        print("weather_worker error:", exc)


def alarm_worker(producer: AlarmProducer, interval_sec: int) -> None:
    """
    Produces rare alarms. This worker runs much less frequently (ALARM_INTERVAL_SEC).
    It still iterates through configured sensors and only emits when generator returns an event.
    """
    try:
        while not stop_event.is_set():
            ts = _utc_now()
            producer.run(start_time=ts, delta=timedelta(seconds=interval_sec), iterations=1)
            if stop_event.wait(interval_sec):
                break
    except Exception as exc:
        print("alarm_worker error:", exc)


# -------------------------
# Main entrypoint
# -------------------------
def main() -> None:
    print("Starting producers. Press Ctrl+C to stop.")
    threads: List[threading.Thread] = []

    # sensor worker (handles both measurement and health via sensor_producer.run)
    t_sensor = threading.Thread(
        target=sensor_worker, name="sensor-worker", args=(sensor_producer, SENSOR_INTERVAL_SEC), daemon=True
    )
    threads.append(t_sensor)

    # weather worker
    t_weather = threading.Thread(
        target=weather_worker, name="weather-worker", args=(weather_producer, WEATHER_INTERVAL_SEC), daemon=True
    )
    threads.append(t_weather)

    # alarm worker (lower frequency)
    t_alarm = threading.Thread(
        target=alarm_worker, name="alarm-worker", args=(alarm_producer, ALARM_INTERVAL_SEC), daemon=True
    )
    threads.append(t_alarm)

    # Start all threads
    for t in threads:
        t.start()

    try:
        # Keep main thread alive while workers run
        while not stop_event.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nKeyboardInterrupt received — shutting down producers...")
        stop_event.set()
    finally:
        # Wait briefly for threads to finish
        for t in threads:
            t.join(timeout=2.0)
        print("Shutdown complete.")


if __name__ == "__main__":
    main()