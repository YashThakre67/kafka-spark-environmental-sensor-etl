## Architecture Diagram

![Kafka-Spark Environmental Sensor ETL Architecture](images/pipeline_architecture.png)

# Pipeline Explanation

This document explains the end-to-end design of the **Kafka-Spark Environmental Sensor ETL Pipeline**.

The project implements a real-time streaming ETL pipeline using Apache Kafka and Apache Spark. It processes two independent environmental sensor streams:

1. Weather sensor data
2. Particulate matter sensor data

The pipeline consumes both streams from Kafka, parses JSON records, filters invalid records, applies transformation logic, groups records into time windows, joins the two streams, calculates correlation, serializes the result as JSON, and writes the final output to a file sink.

---

## End-to-End Pipeline Flow

```text
Weather Data Source
        ↓
Kafka Topic: weather-sensors
        ↓
Parse JSON
        ↓
Filter
        ↓
Window Aggregation
        ↓
Window Collect
        ↓
Join & Align Streams
        ↓
Correlate
        ↓
Collect Results
        ↓
Serialize JSON
        ↓
File Sink


Particulate Matter Data Source
        ↓
Kafka Topic: particulate-matter
        ↓
Parse JSON
        ↓
Filter
        ↓
Window Aggregation
        ↓
Window Collect
        ↓
Join & Align Streams
