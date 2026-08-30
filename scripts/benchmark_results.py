import argparse
import csv
import json
from pathlib import Path
from statistics import mean


def read_spark_json_output(output_path: Path):
    """
    Reads Spark text output from batch folders.

    Expected Spark output structure:
    output/single_node/batch_0/part-00000
    output/distributed/batch_0/part-00000
    """
    records = []

    if not output_path.exists():
        print(f"Output path does not exist: {output_path}")
        return records

    part_files = sorted(output_path.rglob("part-*"))

    for part_file in part_files:
        with part_file.open("r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()

                if not line:
                    continue

                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    print(f"Skipping invalid JSON line in {part_file}")

    return records


def summarize_records(
    execution_name: str,
    records: list,
    elapsed_seconds: float | None = None,
):
    """
    Creates a benchmark summary for one execution mode.
    """
    record_count = len(records)

    batch_ids = sorted(
        {
            record.get("batch_id")
            for record in records
            if record.get("batch_id") is not None
        }
    )

    window_starts = sorted(
        {
            record.get("window_start")
            for record in records
            if record.get("window_start") is not None
        }
    )

    correlations = [
        record.get("temperature_pm25_correlation")
        for record in records
        if record.get("temperature_pm25_correlation") is not None
    ]

    execution_modes = sorted(
        {
            record.get("execution_mode")
            for record in records
            if record.get("execution_mode") is not None
        }
    )

    throughput = None

    if elapsed_seconds and elapsed_seconds > 0:
        throughput = round(record_count / elapsed_seconds, 3)

    return {
        "execution_name": execution_name,
        "execution_modes_found": ", ".join(execution_modes),
        "output_records": record_count,
        "batches": len(batch_ids),
        "unique_windows": len(window_starts),
        "first_window": window_starts[0] if window_starts else "",
        "last_window": window_starts[-1] if window_starts else "",
        "avg_correlation": round(mean(correlations), 4) if correlations else "",
        "elapsed_seconds": elapsed_seconds if elapsed_seconds else "",
        "throughput_records_per_second": throughput if throughput else "",
    }


def write_summary_csv(summary_rows: list, output_file: Path):
    """
    Writes benchmark summary rows into a CSV file.
    """
    output_file.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "execution_name",
        "execution_modes_found",
        "output_records",
        "batches",
        "unique_windows",
        "first_window",
        "last_window",
        "avg_correlation",
        "elapsed_seconds",
        "throughput_records_per_second",
    ]

    with output_file.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(summary_rows)


def print_summary(summary_rows: list):
    """
    Prints benchmark results in a readable format.
    """
    print("\nBenchmark Summary")
    print("=" * 80)

    for row in summary_rows:
        print(f"\nExecution: {row['execution_name']}")
        print(f"Execution mode found: {row['execution_modes_found']}")
        print(f"Output records: {row['output_records']}")
        print(f"Batches: {row['batches']}")
        print(f"Unique windows: {row['unique_windows']}")
        print(f"First window: {row['first_window']}")
        print(f"Last window: {row['last_window']}")
        print(f"Average correlation: {row['avg_correlation']}")
        print(f"Elapsed seconds: {row['elapsed_seconds']}")
        print(f"Throughput records/sec: {row['throughput_records_per_second']}")

    if len(summary_rows) == 2:
        single = summary_rows[0]
        distributed = summary_rows[1]

        single_tp = single.get("throughput_records_per_second")
        distributed_tp = distributed.get("throughput_records_per_second")

        if single_tp and distributed_tp:
            speedup = round(float(distributed_tp) / float(single_tp), 3)
            print("\nPerformance Comparison")
            print("=" * 80)
            print(f"Distributed speedup over single-node: {speedup}x")


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark single-node and distributed Spark ETL outputs."
    )

    parser.add_argument(
        "--single-path",
        default="output/single_node",
        help="Path to single-node Spark output.",
    )

    parser.add_argument(
        "--distributed-path",
        default="output/distributed",
        help="Path to distributed Spark output.",
    )

    parser.add_argument(
        "--single-elapsed-seconds",
        type=float,
        default=None,
        help="Optional elapsed runtime for single-node execution.",
    )

    parser.add_argument(
        "--distributed-elapsed-seconds",
        type=float,
        default=None,
        help="Optional elapsed runtime for distributed execution.",
    )

    parser.add_argument(
        "--output-csv",
        default="results/performance_summary.csv",
        help="CSV file where benchmark results will be written.",
    )

    args = parser.parse_args()

    single_records = read_spark_json_output(Path(args.single_path))
    distributed_records = read_spark_json_output(Path(args.distributed_path))

    summary_rows = [
        summarize_records(
            execution_name="single_node",
            records=single_records,
            elapsed_seconds=args.single_elapsed_seconds,
        ),
        summarize_records(
            execution_name="distributed_4_workers",
            records=distributed_records,
            elapsed_seconds=args.distributed_elapsed_seconds,
        ),
    ]

    write_summary_csv(summary_rows, Path(args.output_csv))
    print_summary(summary_rows)

    print(f"\nBenchmark CSV written to: {args.output_csv}")


if __name__ == "__main__":
    main()
