import json
import time
import argparse
from pathlib import Path
from kafka import KafkaProducer


def create_producer(bootstrap_server: str) -> KafkaProducer:
    """
    Creates a Kafka producer that sends JSON records as UTF-8 encoded messages.
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_server,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8") if key else None,
    )


def read_json_lines(file_path: str):
    """
    Reads JSON records from a JSON lines file.
    Each line should contain one JSON object.
    """
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    with path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            line = line.strip()

            if not line:
                continue

            try:
                yield json.loads(line)
            except json.JSONDecodeError as error:
                print(f"Skipping invalid JSON at line {line_number}: {error}")


def publish_weather_data(
    bootstrap_server: str,
    topic: str,
    input_file: str,
    delay_seconds: float,
    repeat: bool,
):
    """
    Publishes weather sensor records to a Kafka topic.
    """
    producer = create_producer(bootstrap_server)

    print(f"Starting weather producer...")
    print(f"Kafka bootstrap server: {bootstrap_server}")
    print(f"Topic: {topic}")
    print(f"Input file: {input_file}")

    try:
        while True:
            record_count = 0

            for record in read_json_lines(input_file):
                sensor_id = record.get("sensor_id", "weather_sensor")

                producer.send(
                    topic=topic,
                    key=sensor_id,
                    value=record,
                )

                record_count += 1
                print(f"Sent weather record {record_count}: {record}")

                if delay_seconds > 0:
                    time.sleep(delay_seconds)

            producer.flush()
            print(f"Finished publishing {record_count} weather records.")

            if not repeat:
                break

            print("Repeating weather data stream...")

    except KeyboardInterrupt:
        print("Weather producer stopped manually.")

    finally:
        producer.flush()
        producer.close()
        print("Weather producer closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Kafka producer for weather sensor data."
    )

    parser.add_argument(
        "--bootstrap-server",
        default="localhost:9092",
        help="Kafka bootstrap server address.",
    )

    parser.add_argument(
        "--topic",
        default="weather-sensors",
        help="Kafka topic for weather sensor data.",
    )

    parser.add_argument(
        "--input-file",
        default="data/weather_sample.json",
        help="Path to the weather JSON input file.",
    )

    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Delay in seconds between records.",
    )

    parser.add_argument(
        "--repeat",
        action="store_true",
        help="Continuously repeat the input file.",
    )

    args = parser.parse_args()

    publish_weather_data(
        bootstrap_server=args.bootstrap_server,
        topic=args.topic,
        input_file=args.input_file,
        delay_seconds=args.delay,
        repeat=args.repeat,
    )
