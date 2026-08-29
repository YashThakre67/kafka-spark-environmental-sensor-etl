# Kafka-Spark Environmental Sensor ETL Pipeline

This project implements a real-time streaming ETL pipeline using Apache Kafka and Apache Spark. It simulates two environmental sensor streams: weather sensor data and particulate matter sensor data. Spark consumes both Kafka streams, parses JSON records, applies transformations, filters invalid data, performs user-defined processing, joins the streams, calculates correlation, and writes the processed output to a sink.

The project also compares the same pipeline in two execution modes:

- Single-node Spark execution
- Distributed Spark execution with one Spark master and four Spark workers

The goal of this project is to demonstrate real-time data ingestion, stream processing, distributed data processing, and performance benchmarking using Kafka, PySpark, Docker, and Spark.

---

## Project Overview

The pipeline is designed around two independent Kafka sources:

1. **Weather sensor stream**
   - Temperature
   - Humidity
   - Timestamp

2. **Particulate matter sensor stream**
   - PM2.5 values
   - PM10 values
   - Timestamp

Both streams are processed by Spark and aligned for correlation analysis between weather conditions and air-quality measurements.

---

## Architecture

```text
Weather Data Producer
        |
        v
Kafka Topic: weather-sensors
        |
        v
Spark Streaming ETL Job
        |
        |---- Parse JSON
        |---- Map transformations
        |---- Filter invalid records
        |---- Apply UDF logic
        |---- Window-based processing
        |---- Join weather and particulate matter streams
        |---- Calculate correlation
        |
        v
Output Sink


Particulate Matter Producer
        |
        v
Kafka Topic: particulate-matter
        |
        v
Spark Streaming ETL Job
