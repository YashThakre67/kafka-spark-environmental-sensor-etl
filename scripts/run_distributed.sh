#!/bin/bash

set -e

echo "Starting Kafka-Spark Environmental Sensor ETL pipeline in DISTRIBUTED mode..."

echo "Starting Kafka, Spark master, and Spark worker containers..."
docker compose up -d

echo "Waiting for Kafka and Spark cluster to initialize..."
sleep 15

echo "Creating Kafka topics..."
bash scripts/create_topics.sh

echo "Cleaning previous distributed output and checkpoint data..."
docker exec spark-master rm -rf /opt/spark-output/distributed
docker exec spark-master rm -rf /tmp/spark-checkpoints/environmental-etl-distributed

echo "Submitting distributed Spark ETL job..."

docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark-apps/environmental_etl_distributed.py \
  --spark-master spark://spark-master:7077 \
  --bootstrap-server kafka:9093 \
  --output-path /opt/spark-output/distributed \
  --checkpoint-path /tmp/spark-checkpoints/environmental-etl-distributed \
  --trigger-interval "5 seconds" \
  --partitions 4
