import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


WEATHER_TOPIC = "weather-sensors"
PM_TOPIC = "particulate-matter"


def parse_json_record(json_text: str):
    """
    Parses one JSON string into a Python dictionary.
    Invalid JSON records are skipped.
    """
    try:
        return json.loads(json_text)
    except json.JSONDecodeError:
        return None


def parse_timestamp(timestamp_text: str) -> datetime:
    """
    Parses an ISO-8601 timestamp string into a timezone-aware datetime object.
    """
    return datetime.fromisoformat(timestamp_text.replace("Z", "+00:00"))


def get_window_start(timestamp_text: str, window_seconds: int = 10) -> str:
    """
    Assigns an event timestamp to a fixed-size time window.
    Example: 00:00:05 and 00:00:09 belong to the same 10-second window.
    """
    event_time = parse_timestamp(timestamp_text)
    epoch_seconds = int(event_time.timestamp())
    window_epoch = (epoch_seconds // window_seconds) * window_seconds

    return (
        datetime.fromtimestamp(window_epoch, timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def is_valid_weather_record(record: dict) -> bool:
    """
    Filters invalid weather records.
    """
    if record is None:
        return False

    required_fields = ["sensor_id", "timestamp", "temperature", "humidity"]

    return all(field in record for field in required_fields)


def is_valid_pm_record(record: dict) -> bool:
    """
    Filters invalid particulate matter records.
    """
    if record is None:
        return False

    required_fields = ["sensor_id", "timestamp", "pm2_5", "pm10"]

    return all(field in record for field in required_fields)


def calculate_temperature_humidity_index(temperature: float, humidity: float) -> float:
    """
    UDF-style calculation used to enrich weather records.
    This is a simple derived feature for demonstration.
    """
    return round(temperature + (0.05 * humidity), 3)


def classify_air_quality(pm2_5: float) -> str:
    """
    UDF-style classification for particulate matter values.
    """
    if pm2_5 <= 12:
        return "good"
    if pm2_5 <= 35.4:
        return "moderate"
    if pm2_5 <= 55.4:
        return "unhealthy_sensitive"
    return "unhealthy"


def transform_weather_record(record: dict) -> dict:
    """
    Applies map-style transformation and UDF-style enrichment to weather records.
    """
    temperature = float(record["temperature"])
    humidity = float(record["humidity"])

    return {
        "sensor_id": record["sensor_id"],
        "timestamp": record["timestamp"],
        "window_start": get_window_start(record["timestamp"]),
        "temperature": temperature,
        "humidity": humidity,
        "temperature_humidity_index": calculate_temperature_humidity_index(
            temperature, humidity
        ),
    }


def transform_pm_record(record: dict) -> dict:
    """
    Applies map-style transformation and UDF-style enrichment to particulate matter records.
    """
    pm2_5 = float(record["pm2_5"])
    pm10 = float(record["pm10"])

    return {
        "sensor_id": record["sensor_id"],
        "timestamp": record["timestamp"],
        "window_start": get_window_start(record["timestamp"]),
        "pm2_5": pm2_5,
        "pm10": pm10,
        "air_quality_level": classify_air_quality(pm2_5),
    }


def aggregate_weather_by_window(weather_rdd):
    """
    Aggregates weather records by event-time window.
    """
    return (
        weather_rdd
        .map(
            lambda record: (
                record["window_start"],
                (
                    record["temperature"],
                    record["humidity"],
                    record["temperature_humidity_index"],
                    1,
                ),
            )
        )
        .reduceByKey(
            lambda left, right: (
                left[0] + right[0],
                left[1] + right[1],
                left[2] + right[2],
                left[3] + right[3],
            )
        )
        .mapValues(
            lambda values: {
                "avg_temperature": round(values[0] / values[3], 3),
                "avg_humidity": round(values[1] / values[3], 3),
                "avg_temperature_humidity_index": round(values[2] / values[3], 3),
                "weather_record_count": values[3],
            }
        )
    )


def aggregate_pm_by_window(pm_rdd):
    """
    Aggregates particulate matter records by event-time window.
    """
    return (
        pm_rdd
        .map(
            lambda record: (
                record["window_start"],
                (
                    record["pm2_5"],
                    record["pm10"],
                    1,
                ),
            )
        )
        .reduceByKey(
            lambda left, right: (
                left[0] + right[0],
                left[1] + right[1],
                left[2] + right[2],
            )
        )
        .mapValues(
            lambda values: {
                "avg_pm2_5": round(values[0] / values[2], 3),
                "avg_pm10": round(values[1] / values[2], 3),
                "pm_record_count": values[2],
            }
        )
    )


def pearson_correlation(pairs_rdd):
    """
    Calculates Pearson correlation between average temperature and average PM2.5 values.
    """
    stats = pairs_rdd.aggregate(
        (0, 0.0, 0.0, 0.0, 0.0, 0.0),
        lambda acc, pair: (
            acc[0] + 1,
            acc[1] + pair[0],
            acc[2] + pair[1],
            acc[3] + pair[0] * pair[0],
            acc[4] + pair[1] * pair[1],
            acc[5] + pair[0] * pair[1],
        ),
        lambda left, right: (
            left[0] + right[0],
            left[1] + right[1],
            left[2] + right[2],
            left[3] + right[3],
            left[4] + right[4],
            left[5] + right[5],
        ),
    )

    count, sum_x, sum_y, sum_xx, sum_yy, sum_xy = stats

    if count < 2:
        return None

    numerator = (count * sum_xy) - (sum_x * sum_y)
    denominator_x = (count * sum_xx) - (sum_x * sum_x)
    denominator_y = (count * sum_yy) - (sum_y * sum_y)

    denominator = math.sqrt(denominator_x * denominator_y)

    if denominator == 0:
        return None

    return round(numerator / denominator, 4)


def build_joined_output(record):
    """
    Builds the final output record after joining weather and particulate matter windows.
    """
    window_start, joined_values = record
    weather_values, pm_values = joined_values

    return {
        "window_start": window_start,
        "avg_temperature": weather_values["avg_temperature"],
        "avg_humidity": weather_values["avg_humidity"],
        "avg_temperature_humidity_index": weather_values[
            "avg_temperature_humidity_index"
        ],
        "avg_pm2_5": pm_values["avg_pm2_5"],
        "avg_pm10": pm_values["avg_pm10"],
        "weather_record_count": weather_values["weather_record_count"],
        "pm_record_count": pm_values["pm_record_count"],
    }


def process_micro_batch(batch_df, batch_id: int, output_path: str):
    """
    Processes one Spark micro-batch.

    Data flow:
    Kafka streaming DataFrame
        -> foreachBatch micro-batch DataFrame
        -> RDD[String]
        -> RDD[dict]
        -> RDD transformations
        -> joined analytical output
    """
    if batch_df.rdd.isEmpty():
        print(f"Batch {batch_id}: empty batch")
        return

    print(f"Processing batch {batch_id}")

    weather_json_rdd = (
        batch_df
        .filter(col("topic") == WEATHER_TOPIC)
        .selectExpr("CAST(value AS STRING) AS json_value")
        .rdd
        .map(lambda row: row["json_value"])
    )

    pm_json_rdd = (
        batch_df
        .filter(col("topic") == PM_TOPIC)
        .selectExpr("CAST(value AS STRING) AS json_value")
        .rdd
        .map(lambda row: row["json_value"])
    )

    weather_rdd = (
        weather_json_rdd
        .map(parse_json_record)
        .filter(is_valid_weather_record)
        .map(transform_weather_record)
    )

    pm_rdd = (
        pm_json_rdd
        .map(parse_json_record)
        .filter(is_valid_pm_record)
        .map(transform_pm_record)
    )

    weather_window_rdd = aggregate_weather_by_window(weather_rdd)
    pm_window_rdd = aggregate_pm_by_window(pm_rdd)

    joined_rdd = (
        weather_window_rdd
        .join(pm_window_rdd)
        .map(build_joined_output)
        .cache()
    )

    output_count = joined_rdd.count()

    if output_count == 0:
        print(f"Batch {batch_id}: no joined output records")
        return

    correlation_input_rdd = joined_rdd.map(
        lambda record: (
            float(record["avg_temperature"]),
            float(record["avg_pm2_5"]),
        )
    )

    batch_correlation = pearson_correlation(correlation_input_rdd)

    final_output_rdd = joined_rdd.map(
        lambda record: {
            **record,
            "batch_id": batch_id,
            "temperature_pm25_correlation": batch_correlation,
            "execution_mode": "single_node",
        }
    )

    batch_output_path = f"{output_path}/batch_{batch_id}"

    final_output_rdd.map(
        lambda record: json.dumps(record, sort_keys=True)
    ).saveAsTextFile(batch_output_path)

    print(f"Batch {batch_id}: wrote {output_count} joined records")


def main():
    parser = argparse.ArgumentParser(
        description="Single-node Kafka-Spark environmental sensor ETL pipeline."
    )

    parser.add_argument(
        "--bootstrap-server",
        default="kafka:9093",
        help="Kafka bootstrap server used by Spark inside Docker.",
    )

    parser.add_argument(
        "--output-path",
        default="/opt/spark-output/single_node",
        help="Output path for processed results.",
    )

    parser.add_argument(
        "--checkpoint-path",
        default="/tmp/spark-checkpoints/environmental-etl-single-node",
        help="Checkpoint path for Spark streaming.",
    )

    parser.add_argument(
        "--trigger-interval",
        default="5 seconds",
        help="Spark processing-time trigger interval.",
    )

    args = parser.parse_args()

    Path(args.output_path).mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("Kafka-Spark Environmental Sensor ETL - Single Node")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    kafka_stream_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_server)
        .option("subscribe", f"{WEATHER_TOPIC},{PM_TOPIC}")
        .option("startingOffsets", "latest")
        .load()
    )

    query = (
        kafka_stream_df.writeStream
        .trigger(processingTime=args.trigger_interval)
        .option("checkpointLocation", args.checkpoint_path)
        .foreachBatch(
            lambda batch_df, batch_id: process_micro_batch(
                batch_df=batch_df,
                batch_id=batch_id,
                output_path=args.output_path,
            )
        )
        .start()
    )

    print("Single-node environmental sensor ETL pipeline started.")
    print(f"Kafka bootstrap server: {args.bootstrap_server}")
    print(f"Trigger interval: {args.trigger_interval}")
    print(f"Output path: {args.output_path}")

    query.awaitTermination()


if __name__ == "__main__":
    main()
