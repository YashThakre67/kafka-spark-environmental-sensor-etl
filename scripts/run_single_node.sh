#!/bin/bash
#!/bin/bash

set -e

echo "Starting Kafka-Spark Environmental Sensor ETL pipeline in SINGLE-NODE mode..."

echo "Starting required Docker containers..."
docker compose up -d kafka spark-master

echo "Waiting for Kafka and Spark to initialize..."
sleep 10

echo "Creating Kafka topics..."
bash scripts/create_topics.sh

echo "Cleaning previous single-node output and checkpoint data..."
docker exec spark-master rm -rf /opt/spark-output/single_node
docker exec spark-master rm -rf /tmp/spark-checkpoints/environmental-etl-single-node

echo "Submitting single-node Spark ETL job..."

docker exec spark-master /opt/spark/bin/spark-submit \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark-apps/environmental_etl_single_node.py \
  --bootstrap-server kafka:9093 \
  --output-path /opt/spark-output/single_node \
  --checkpoint-path /tmp/spark-checkpoints/environmental-etl-single-node \
  --trigger-interval "5 seconds"

