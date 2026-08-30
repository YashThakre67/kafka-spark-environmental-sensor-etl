#!/bin/bash
# TODO: Script will be added.
#!/bin/bash

# Create Kafka topics for the environmental sensor ETL pipeline.
# This script assumes that the Kafka container is already running
# using docker-compose.yml.

KAFKA_CONTAINER="kafka"
BOOTSTRAP_SERVER="kafka:9093"

WEATHER_TOPIC="weather-sensors"
PM_TOPIC="particulate-matter"

echo "Creating Kafka topics..."

docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --topic $WEATHER_TOPIC \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --partitions 1 \
  --replication-factor 1

docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \
  --create \
  --if-not-exists \
  --topic $PM_TOPIC \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --partitions 1 \
  --replication-factor 1

echo "Kafka topics created successfully."

echo "Available Kafka topics:"
docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server $BOOTSTRAP_SERVER
