Here is a `COMMANDS.md` file designed to be a "Cheat Sheet" for operating, debugging, and maintaining your Air Pulse pipeline.

It focuses on interacting with the Docker containers to check Kafka, monitor Spark, and manage your data.

***

# 🛠️ Air Pulse: Operations & Debugging Guide

This document contains useful commands for managing Kafka topics, debugging data streams, and monitoring the Spark application.

> **Note:** All Kafka commands below use `docker exec` to run directly inside the `broker` container. This ensures networking (`broker:29092`) works correctly regardless of your host OS.

## 🔍 Kafka Management

### 1. List All Topics
Check which topics have been created by the producers.

```bash
docker exec -it broker kafka-topics --list --bootstrap-server broker:29092
```

### 2. Read Messages (Consumer)
View the raw JSON data landing in a specific topic in real-time.

**Weather Context:**
```bash
docker exec -it broker kafka-console-consumer --topic weather.context --bootstrap-server broker:29092 --from-beginning
```

**Sensor Measurements:**
```bash
docker exec -it broker kafka-console-consumer --topic sensor.measurements --bootstrap-server broker:29092 --from-beginning
```

**Fire Alerts:**
```bash
docker exec -it broker kafka-console-consumer --topic alerts.fire --bootstrap-server broker:29092 --from-beginning
```

*(Press `Ctrl + C` to exit the consumer)*

### 3. Clear/Reset a Topic
The easiest way to "clear" data in a development environment is to delete the topic. Kafka (or the Producer) will recreate it automatically when new data is sent (if `auto.create.topics.enable` is true, which is default).

**Delete a specific topic:**
```bash
docker exec -it broker kafka-topics --delete --topic weather.context --bootstrap-server broker:29092
```

**Delete ALL project topics (Reset):**
```bash
docker exec -it broker kafka-topics --delete --topic sensor.measurements --bootstrap-server broker:29092
docker exec -it broker kafka-topics --delete --topic sensor.health --bootstrap-server broker:29092
docker exec -it broker kafka-topics --delete --topic air.quality --bootstrap-server broker:29092
docker exec -it broker kafka-topics --delete --topic weather.context --bootstrap-server broker:29092
docker exec -it broker kafka-topics --delete --topic weather.wind --bootstrap-server broker:29092
docker exec -it broker kafka-topics --delete --topic alerts.fire --bootstrap-server broker:29092
```

### 4. Check Topic Details
See partition count and replication status.
```bash
docker exec -it broker kafka-topics --describe --topic sensor.measurements --bootstrap-server broker:29092
```

---

## 📈 Monitoring Spark

### Spark Master UI
Access the Spark Master web interface to see connected workers and running applications.
*   **URL:** [http://localhost:8080/](http://localhost:8080/)

### Spark Application UI (Streaming Statistics)
Once your application (`AirPulseStreamingJob`) is running, it will appear in the Master UI under "Running Applications".
1.  Click the **App ID** (e.g., `app-20240101123456-0000`).
2.  This opens the detailed Application UI (often redirected to a random internal port).
    *   *Note:* If the link tries to go to a docker hostname (like `spark-master:4040`), you may need to manually replace `spark-master` with `localhost` in your browser URL bar.

---

## ☁️ AWS S3 / Storage Verification

If you have the AWS CLI installed on your host machine, you can verify that Spark is successfully writing Parquet files.

**List Data Folders:**
```bash
aws s3 ls s3://<YOUR_BUCKET_NAME>/data/
```

**List Checkpoints (Streaming State):**
```bash
aws s3 ls s3://<YOUR_BUCKET_NAME>/checkpoints/
```

---

## ℹ️ Simulation Data Disclaimer

The data generation logic located in `producer/generator_functions/` uses mathematical equations and randomization functions.

*   **Source:** These algorithms were assisted by AI (ChatGPT) to create plausible-looking patterns (diurnal cycles, seasonality, traffic peaks).
*   **Accuracy:** This data is for **simulation and engineering testing purposes only**. It does not reflect real-world atmospheric physics or actual sensor readings.
*   **Purpose:** To provide a robust dataset for testing streaming pipelines, windowing aggregations, and alerting logic.