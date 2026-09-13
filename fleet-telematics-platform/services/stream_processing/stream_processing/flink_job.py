"""PyFlink reference implementation of the same pipeline as streaming_job.py
(Kafka -> H3 index -> 5-minute windowed density -> ClickHouse), using the
Table API + SQL.

Why this exists alongside the PySpark job: Spark's micro-batch model
naturally maps onto batched ClickHouse `foreachBatch` inserts (the primary
implementation), but a principal architect's stack decision should be able
to name the trade-off, not just assert it. This job demonstrates the same
logic under Flink's true event-at-a-time engine:

- Lower steady-state latency: Flink emits window results as soon as the
  window's watermark closes it, rather than waiting for Spark's next
  micro-batch trigger tick.
- Higher operational cost: a Flink cluster (JobManager/TaskManager) and
  checkpoint/state-backend tuning to operate, versus Spark's simpler
  driver/executor model that this project already runs for the primary job.

Speed-anomaly detection (the stream-stream join in streaming_job.py) is
*not* re-implemented here -- Flink's Table API supports interval joins with
the same semantics, but duplicating that logic in two engines adds
maintenance cost without adding architectural insight beyond what the
windowed-density comparison already shows.

Run (after `poetry install --with flink`):

    poetry run run-flink-job
"""

from __future__ import annotations

import structlog
from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.udf import ScalarFunction, udf

from stream_processing.config import config
from stream_processing.h3_utils import cell_int_to_parent_int, latlng_to_cell_int

log = structlog.get_logger()


class H3FineIndexUDF(ScalarFunction):
    def eval(self, lat: float, lon: float) -> int:
        return latlng_to_cell_int(lat, lon, config.h3_resolution_fine)


class H3ParentIndexUDF(ScalarFunction):
    def eval(self, cell: int) -> int:
        return cell_int_to_parent_int(cell, config.h3_resolution_coarse)


def build_table_env() -> TableEnvironment:
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)
    t_env.get_config().set(
        "pipeline.jars",
        "file:///opt/flink/lib/flink-sql-connector-kafka.jar;"
        "file:///opt/flink/lib/flink-connector-jdbc.jar;"
        "file:///opt/flink/lib/clickhouse-jdbc.jar",
    )
    return t_env


def register_source_table(t_env: TableEnvironment) -> None:
    t_env.execute_sql(
        f"""
        CREATE TABLE raw_vehicle_positions (
            vehicle_id   STRING,
            route_id     STRING,
            trip_id      STRING,
            source       STRING,
            lat          DOUBLE,
            lon          DOUBLE,
            speed_mps    FLOAT,
            bearing_deg  FLOAT,
            event_time   TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '{config.watermark_delay}'
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{config.raw_positions_topic}',
            'properties.bootstrap.servers' = '{config.kafka_bootstrap_servers}',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
        """
    )


def register_sink_table(t_env: TableEnvironment) -> None:
    t_env.execute_sql(
        f"""
        CREATE TABLE h3_density_5min (
            window_start   TIMESTAMP(3),
            window_end     TIMESTAMP(3),
            h3_r9          BIGINT,
            h3_r8          BIGINT,
            vehicle_count  BIGINT,
            avg_speed_mps  FLOAT,
            max_speed_mps  FLOAT,
            sample_count   BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:clickhouse://{config.clickhouse_host}:{config.clickhouse_port}/{config.clickhouse_database}',
            'table-name' = 'h3_density_5min',
            'username' = '{config.clickhouse_user}',
            'password' = '{config.clickhouse_password}',
            'sink.buffer-flush.max-rows' = '500',
            'sink.buffer-flush.interval' = '{config.trigger_interval.replace(" ", "")}'
        )
        """
    )


def run() -> None:
    t_env = build_table_env()
    t_env.create_temporary_function(
        "h3_fine_index", udf(H3FineIndexUDF(), result_type=DataTypes.BIGINT())
    )
    t_env.create_temporary_function(
        "h3_parent_index", udf(H3ParentIndexUDF(), result_type=DataTypes.BIGINT())
    )

    register_source_table(t_env)
    register_sink_table(t_env)

    # Same 5-minute / 1-minute-slide density aggregation as
    # streaming_job.py::compute_windowed_density, expressed as Flink SQL
    # over a HOP (sliding) window.
    t_env.execute_sql(
        """
        INSERT INTO h3_density_5min
        SELECT
            window_start,
            window_end,
            h3_r9,
            h3_parent_index(h3_r9) AS h3_r8,
            COUNT(DISTINCT vehicle_id) AS vehicle_count,
            CAST(AVG(speed_mps) AS FLOAT) AS avg_speed_mps,
            CAST(MAX(speed_mps) AS FLOAT) AS max_speed_mps,
            COUNT(*) AS sample_count
        FROM TABLE(
            HOP(
                TABLE (
                    SELECT *, h3_fine_index(lat, lon) AS h3_r9
                    FROM raw_vehicle_positions
                ),
                DESCRIPTOR(event_time),
                INTERVAL '1' MINUTE,
                INTERVAL '5' MINUTE
            )
        )
        GROUP BY window_start, window_end, h3_r9
        """
    ).wait()


def main() -> None:
    run()


if __name__ == "__main__":
    main()
