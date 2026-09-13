# services/stream_processing

The core of the platform: consumes raw vehicle positions from Kafka/Redpanda,
H3-indexes every event, computes rolling 5-minute density, and detects
per-vehicle speed anomalies relative to local traffic — then writes all
three to ClickHouse.

- `stream_processing/streaming_job.py` — primary implementation, PySpark
  Structured Streaming.
- `stream_processing/flink_job.py` — PyFlink Table API reference
  implementation of the density pipeline, included to make the Spark-vs-Flink
  trade-off concrete (see its module docstring).
- `stream_processing/h3_utils.py` — H3 indexing helpers (unit tested in
  `tests/test_h3_utils.py`).

See [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md) §5 for the windowing
and anomaly-detection design.

## Setup

```bash
cd services/stream_processing
poetry install                 # PySpark job
poetry install --with flink    # + PyFlink, for the reference implementation
```

## Run

Requires `infra/docker-compose.yml` running (Redpanda + ClickHouse) and at
least one ingestion producer publishing to `fleet.vehicle_positions.raw`.

```bash
# PySpark (primary)
poetry run run-streaming-job

# or explicitly via spark-submit, with the Kafka connector on the classpath:
poetry run spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    stream_processing/streaming_job.py

# PyFlink (reference implementation, density only)
poetry run run-flink-job
```

## Test

```bash
poetry run pytest
```

## Validating correctness

`infra/clickhouse/init/01_schema.sql` defines `h3_density_5min_validation`,
a view that recomputes 5-minute density directly from
`raw_vehicle_positions` using ClickHouse-native aggregation. After letting
the job run for a few windows:

```sql
SELECT d.window_start, d.h3_r9, d.vehicle_count, v.vehicle_count AS validation_count
FROM fleet_telematics.h3_density_5min_current d
JOIN fleet_telematics.h3_density_5min_validation v
  ON d.window_start = v.window_start AND d.h3_r9 = v.h3_r9
WHERE abs(d.vehicle_count - v.vehicle_count) > 1  -- allow approx_count_distinct's small margin
ORDER BY d.window_start DESC;
```

An empty result set is the correctness bar referenced in PRD.md §7.
