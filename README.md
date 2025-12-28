At the bottom of the file, I have included a section titled **"Project Branding & Image Generation"** containing detailed prompts you can use with tools like Midjourney, DALL-E 3, or Stable Diffusion to generate a project banner or logo.

---

# Air Pulse 🌬️

**Real-time Environmental IoT Data Streaming Pipeline**

Air Pulse is a scalable data engineering project that simulates a smart city IoT network. It generates synthetic environmental data (air quality, weather, wind, fire alarms, and sensor health), streams it through **Apache Kafka**, processes it in real-time using **Apache Spark Structured Streaming**, and stores the results in **AWS S3** in Parquet format for downstream analysis (Data Lake).

## 🏗️ Architecture

The diagram below illustrates the end-to-end flow of data from the IoT producers to the S3 data lake.

![Air Pulse System Architecture](images/AirPulse%20Architecture.png)
_(If you have your diagram saved under a different name or path, please update the link above.)_

**Key Components:**

- **Producers:** Python scripts generating realistic synthetic data with diurnal and seasonal patterns.
- **Ingestion:** Confluent Kafka (KRaft mode) running in Docker.
- **Processing:** PySpark 4.1.0 processing 5 distinct data streams.
- **Storage:** AWS S3 (Partitioned Parquet files).
- **Infrastructure:** AWS CloudFormation for S3 buckets and IAM roles.

## 📂 Data Streams

1.  **Sensor Measurements:** PM2.5, CO2, NO2, Temperature, Humidity, Noise, UV Index.
2.  **Sensor Health:** Battery levels, signal strength, online/offline status.
3.  **Air Quality:** City-level AQI calculations.
4.  **Weather Context:** Temperature, humidity, pressure.
5.  **Wind Data:** Speed and direction.
6.  **Alerts:** Rare high-priority fire alarm events.

## 🚀 Prerequisites

- **Docker & Docker Compose** (Desktop or Engine)
- **Python 3.9+**
- **AWS Account** (Access Key & Secret Key with S3 write permissions)

## 🛠️ Setup & Installation

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/air-pulse.git
cd "Air Pulse"
```

### 2. Configure Environment

Create a `.env` file in the root directory (or use the existing one) to configure Kafka.
Update `jobs/config.py` with your AWS credentials (or better, use Environment Variables).

```python
# jobs/config.py
configuration = {
    "AWS_ACCESS_KEY_ID": "YOUR_ACCESS_KEY",
    "AWS_SECRET_ACCESS_KEY": "YOUR_SECRET_KEY",
    "S3_BUCKET_NAME": "your-created-bucket-name"
}
```

### 3. AWS Infrastructure (Optional)

If you do not have an S3 bucket, deploy the provided CloudFormation template:

1.  Go to the AWS Console -> CloudFormation.
2.  Upload `CloudFormationTemplate/S3Template.yaml`.
3.  Note the **Bucket Name** from the outputs and update your `config.py`.

### 4. Start Infrastructure

Launch Kafka and Spark containers:

```bash
docker-compose up -d
```

## 🏃 Usage

### 1. Install Local Dependencies (for runner script)

```bash
pip install confluent-kafka pyspark
```

### 2. Start the Spark Streaming Job

We use a helper script to submit the job directly to the Docker container network.

````bash
python run_streaming.py
```*This will submit `jobs/streaming.py` to the `spark-master` container, downloading the necessary Maven packages (Hadoop-AWS, Kafka-SQL) automatically.*

### 3. Start Data Producers
Open a new terminal. You will need to create a driver script to run the producers (example logic below) or run them individually if you have a main entry point.

*Example driver (create `run_producers.py`):*
```python
from producer.sensor_producer import SensorProducer
from producer.weather_producer import WeatherProducer
from datetime import datetime, timedelta

# Config
KAFKA_CONF = {'bootstrap.servers': 'localhost:9092'}
SENSORS = ["sensor-01", "sensor-02", "sensor-03"]
LOCATIONS = {"sensor-01": (34.05, -118.25)}
CITIES = [{"city": "Los Angeles", "lat": 34.05, "lon": -118.25}]

# Run
sensor_prod = SensorProducer(SENSORS, LOCATIONS, KAFKA_CONF)
weather_prod = WeatherProducer(CITIES, KAFKA_CONF)

start = datetime.now()
sensor_prod.run(start, timedelta(seconds=1), iterations=1000)
weather_prod.run(start, timedelta(seconds=5), iterations=200)
````

Run it:

```bash
python run_producers.py
```

## 📊 Monitoring

1.  **Spark UI:** Visit `http://localhost:8080` to view the running driver and workers.
2.  **S3:** Check your AWS S3 bucket. You should see folders created: `data/sensor_measurements`, `data/air_quality`, etc., containing Parquet files.
