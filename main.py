import os
from confluent_kafka import SerializingProducer
import simplejson as json
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()



# Air quality (PM2.5, CO₂, NO₂), temperature, humidity, noise levels, UV index.





KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
PM25_TOPIC = os.getenv('PM25_TOPIC', 'air_quality_pm25')
CO2_TOPIC = os.getenv('CO2_TOPIC', 'air_quality_co2')
NO2_TOPIC = os.getenv('NO2_TOPIC', 'air_quality_no2')
TEMP_TOPIC = os.getenv('TEMPERATURE_TOPIC', 'temperature')
HUMIDITY_TOPIC = os.getenv('HUMIDITY_TOPIC', 'humidity')
NOISE_TOPIC = os.getenv('NOISE_TOPIC', 'noise_levels')
UV_TOPIC = os.getenv('UV_INDEX_TOPIC', 'uv_index')


start_time = datetime.now().isoformat()
mean_location = {
    "latitude": float(os.getenv('LOCATION_LATITUDE', '0.0')),
    "longitude": float(os.getenv('LOCATION_LONGITUDE', '0.0'))
}
standard_deviation_location = {
    "latitude": float(os.getenv('LOCATION_STDDEV_LATITUDE', '0.01')),
    "longitude": float(os.getenv('LOCATION_STDDEV_LONGITUDE', '0.01'))
}

def simulate_data(producer, pm25_topic, co2_topic, no2_topic, temp_topic, humidity_topic, noise_topic, uv_topic, mean_loc, stddev_loc, start_time):
    import random
    import time

    while True:
        location = {
            "latitude": random.gauss(mean_loc["latitude"], stddev_loc["latitude"]),
            "longitude": random.gauss(mean_loc["longitude"], stddev_loc["longitude"])
        }
        timestamp = datetime.now().isoformat()

        pm25_data = {
            "timestamp": timestamp,
            "location": location,
            "pm25": round(random.uniform(0, 150), 2)
        }
        co2_data = {
            "timestamp": timestamp,
            "location": location,
            "co2": round(random.uniform(400, 2000), 2)
        }
        no2_data = {
            "timestamp": timestamp,
            "location": location,
            "no2": round(random.uniform(0, 200), 2)
        }
        temp_data = {
            "timestamp": timestamp,
            "location": location,
            "temperature": round(random.uniform(-10, 40), 2)
        }
        humidity_data = {
            "timestamp": timestamp,
            "location": location,
            "humidity": round(random.uniform(0, 100), 2)
        }
        noise_data = {
            "timestamp": timestamp,
            "location": location,
            "noise_level": round(random.uniform(30, 120), 2)
        }
        uv_data = {
            "timestamp": timestamp,
            "location": location,
            "uv_index": round(random.uniform(0, 11), 2)
        }

        producer.produce(pm25_topic, pm25_data)
        producer.produce(co2_topic, co2_data)
        producer.produce(no2_topic, no2_data)
        producer.produce(temp_topic, temp_data)
        producer.produce(humidity_topic, humidity_data)
        producer.produce(noise_topic, noise_data)
        producer.produce(uv_topic, uv_data)

        producer.flush()
        time.sleep(1)  # Simulate data generation interval


if __name__ == "__main__":
    producer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'key.serializer': str.encode,
        'value.serializer': lambda v, ctx: json.dumps(v).encode('utf-8'),
        'error_cb': lambda err: print(f"Kafka Producer error: {err}")   
    }

    producer = SerializingProducer(producer_conf)

    print("Kafka Producer configured with bootstrap servers:", KAFKA_BOOTSTRAP_SERVERS)
    
    try:
        simulate_data(producer, PM25_TOPIC, CO2_TOPIC, NO2_TOPIC, TEMP_TOPIC, HUMIDITY_TOPIC, NOISE_TOPIC, UV_TOPIC, mean_location, standard_deviation_location, start_time)
    except KeyboardInterrupt:
        print("Data simulation interrupted by user.")
    except Exception as e:
        print("An error occurred during data simulation:", str(e))