# services/ingestion

Normalizes live vehicle telemetry from two structurally different sources into
the canonical `VehiclePositionEvent` schema (`ingestion/schemas.py`) and
publishes to the `fleet.vehicle_positions.raw` Kafka/Redpanda topic.

| Producer | Source | Cadence |
|---|---|---|
| `ingestion/gtfs_rt_producer.py` | GTFS-Realtime `VehiclePositions` protobuf feed (any [Transitland](https://www.transitland.org/feeds)-registered agency) | Live poll, default 15s |
| `ingestion/nyc_tlc_producer.py` | [NYC TLC trip records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) (batch Parquet, pickup/dropoff interpolated into GPS pings) | Replayed at a simulated real-time cadence |

## Setup

```bash
cd services/ingestion
poetry install
cp .env.example .env   # then edit FLEET_* values as needed
```

## Run

```bash
# Live GTFS-Realtime feed -> Kafka
poetry run run-gtfs-rt-producer

# NYC TLC batch extract replayed as a live stream -> Kafka
# (download a monthly extract first, e.g. yellow_tripdata_2024-01.parquet,
#  and point FLEET_NYC_TLC_SOURCE_PATH at it)
poetry run run-nyc-tlc-producer
```

If the live GTFS-RT feed is unreachable after retries, `gtfs_rt_producer`
falls back to replaying `fixtures/sample_vehicle_positions.jsonl` so the rest
of the pipeline (H3 indexing, windowed density, anomaly detection,
ClickHouse, dashboard) is still fully demoable offline.

## Key design choice: partitioning by `vehicle_id`

Kafka messages are keyed by `vehicle_id` (`schemas.py::kafka_key`), which
guarantees all events for one vehicle land on the same partition in order.
The stream-processing anomaly detector depends on this — its per-vehicle
speed continuity would be undefined if a vehicle's own event sequence could
arrive out of order across partitions.
