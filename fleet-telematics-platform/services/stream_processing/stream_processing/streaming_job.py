"""Structured Streaming job.

fleet.vehicle_positions.raw (Kafka)
    -> parse JSON
    -> H3-index each event (resolution 9 + resolution 8 parent)
    -> 5-minute / 1-minute-slide windowed density per H3-r9 cell
    -> stream-stream join back onto the raw stream to flag per-event
       speed anomalies (z-score against the vehicle's local H3 cell window)
    -> ClickHouse (raw_vehicle_positions, h3_density_5min,
       speed_anomalies, vehicle_latest_position)

Run locally (after `docker compose up` in infra/ and `poetry install` here):

    poetry run run-streaming-job

or via spark-submit with the Kafka connector package:

    poetry run spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \\
        stream_processing/streaming_job.py

See docs/ARCHITECTURE.md §5 for the windowing/anomaly design rationale.
"""

from __future__ import annotations

import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    approx_count_distinct,
    avg,
    col,
    count,
    expr,
    from_json,
    max as spark_max,
    stddev_pop,
    udf,
    when,
    window,
)
from pyspark.sql.types import LongType

from stream_processing.config import config
from stream_processing.h3_utils import cell_int_to_parent_int, latlng_to_cell_int
from stream_processing.schemas import VEHICLE_POSITION_SCHEMA

log = structlog.get_logger()


def build_spark_session(app_name: str = "fleet-telematics-streaming") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")  # laptop-scale default; tune per cluster
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        .getOrCreate()
    )


def read_raw_stream(spark: SparkSession) -> DataFrame:
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", config.raw_positions_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = kafka_df.select(
        from_json(col("value").cast("string"), VEHICLE_POSITION_SCHEMA).alias("event")
    ).select("event.*")
    return parsed.filter(col("event_time").isNotNull())


def add_h3_columns(df: DataFrame) -> DataFrame:
    """H3-index every event at write time (see ARCHITECTURE.md §4 for why
    resolution is decided here, not at query time)."""
    h3_fine_udf = udf(
        lambda lat, lon: latlng_to_cell_int(lat, lon, config.h3_resolution_fine), LongType()
    )
    h3_parent_udf = udf(
        lambda cell: cell_int_to_parent_int(cell, config.h3_resolution_coarse), LongType()
    )
    return df.withColumn("h3_r9", h3_fine_udf(col("lat"), col("lon"))).withColumn(
        "h3_r8", h3_parent_udf(col("h3_r9"))
    )


def compute_windowed_density(indexed_df: DataFrame) -> DataFrame:
    """5-minute / 1-minute-slide vehicle density + speed stats per H3-r9
    cell. `approx_count_distinct` keeps per-window state bounded (see
    ARCHITECTURE.md §5) -- exact distinct would require retaining every
    vehicle_id seen in the window, unbounded as fleet size grows."""
    return (
        indexed_df.withWatermark("event_time", config.watermark_delay)
        .groupBy(
            window(col("event_time"), config.window_duration, config.window_slide),
            col("h3_r9"),
            col("h3_r8"),
        )
        .agg(
            approx_count_distinct("vehicle_id").alias("vehicle_count"),
            avg("speed_mps").cast("float").alias("avg_speed_mps"),
            spark_max("speed_mps").alias("max_speed_mps"),
            stddev_pop("speed_mps").alias("stddev_speed_mps"),
            count("*").alias("sample_count"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "h3_r9",
            "h3_r8",
            "vehicle_count",
            "avg_speed_mps",
            "max_speed_mps",
            "stddev_speed_mps",
            "sample_count",
        )
    )


def detect_speed_anomalies(indexed_df: DataFrame, windowed_stats: DataFrame) -> DataFrame:
    """Per-event anomaly flag via a stream-stream interval join: each raw
    event is enriched with the mean/stddev of its *own* H3 cell's current
    window, then flagged if its z-score clears the threshold.

    Both sides are watermarked, which bounds the join's buffered state to
    events within the watermark delay -- see ARCHITECTURE.md §5.
    """
    raw = indexed_df.withWatermark("event_time", config.watermark_delay).alias("raw")
    stats = windowed_stats.withWatermark("window_end", config.watermark_delay).alias("stats")

    joined = raw.join(
        stats,
        expr(
            """
            raw.h3_r9 = stats.h3_r9 AND
            raw.event_time >= stats.window_start AND
            raw.event_time < stats.window_end
            """
        ),
    )

    z_score_expr = (col("raw.speed_mps") - col("stats.avg_speed_mps")) / col("stats.stddev_speed_mps")

    scored = joined.withColumn("z_score", z_score_expr).filter(
        (col("stats.stddev_speed_mps") > 0)
        & (col("stats.sample_count") >= config.anomaly_min_sample_count)
        & (spark_abs(col("z_score")) >= config.anomaly_z_warning)
    )

    return scored.select(
        col("raw.event_time").alias("alert_time"),
        col("raw.vehicle_id").alias("vehicle_id"),
        col("raw.route_id").alias("route_id"),
        col("raw.h3_r9").alias("h3_r9"),
        col("raw.h3_r8").alias("h3_r8"),
        col("raw.lat").alias("lat"),
        col("raw.lon").alias("lon"),
        col("raw.speed_mps").alias("speed_mps"),
        col("stats.avg_speed_mps").alias("window_mean_speed_mps"),
        col("stats.stddev_speed_mps").alias("window_stddev_speed"),
        col("z_score"),
        col("stats.sample_count").alias("window_sample_count"),
        when(spark_abs(col("z_score")) >= config.anomaly_z_critical, "CRITICAL")
        .otherwise("WARNING")
        .alias("severity"),
    )


def get_clickhouse_client():  # type: ignore[no-untyped-def]
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=config.clickhouse_host,
        port=config.clickhouse_port,
        database=config.clickhouse_database,
        username=config.clickhouse_user,
        password=config.clickhouse_password,
    )


def make_clickhouse_sink(table: str, columns: list[str]):  # type: ignore[no-untyped-def]
    """Returns a foreachBatch function inserting a micro-batch DataFrame
    into `table`. Batched via clickhouse-connect (HTTP interface) rather
    than row-by-row -- ClickHouse throughput comes from batch size, and the
    Spark trigger interval already gives us a natural batch boundary."""

    def _sink(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        rows = [tuple(r[c] for c in columns) for r in batch_df.select(*columns).collect()]
        client = get_clickhouse_client()
        try:
            client.insert(table, rows, column_names=columns)
            log.info("clickhouse.batch_written", table=table, batch_id=batch_id, rows=len(rows))
        finally:
            client.close()

    return _sink


def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw = read_raw_stream(spark)
    indexed = add_h3_columns(raw)
    windowed_density = compute_windowed_density(indexed)
    # NOTE: windowed_density feeds two separate writeStream queries below
    # (density_query and, via the join, anomaly_query) -- Spark Structured
    # Streaming does not share state across independently-started queries,
    # so the windowed aggregation runs twice per micro-batch. Acceptable at
    # this project's design-point throughput (~500 events/s, see PRD.md
    # §6); at higher scale the fix is to have density_query publish to the
    # optional `fleet.h3_density.5m` Kafka topic (drawn in the architecture
    # diagram) and have the anomaly detector consume its stats side from
    # there instead of recomputing them in-process.
    anomalies = detect_speed_anomalies(indexed, windowed_density)

    raw_query = (
        indexed.select(
            "event_time", "vehicle_id", "route_id", "trip_id", "source",
            "lat", "lon", "speed_mps", "bearing_deg", "h3_r9", "h3_r8",
        )
        .writeStream.foreachBatch(
            make_clickhouse_sink(
                "raw_vehicle_positions",
                ["event_time", "vehicle_id", "route_id", "trip_id", "source",
                 "lat", "lon", "speed_mps", "bearing_deg", "h3_r9", "h3_r8"],
            )
        )
        .option("checkpointLocation", f"{config.checkpoint_location}/raw_positions")
        .trigger(processingTime=config.trigger_interval)
        .start()
    )

    latest_position_query = (
        indexed.select(
            "vehicle_id", "route_id", "event_time", "lat", "lon", "speed_mps", "bearing_deg", "h3_r9", "h3_r8"
        )
        .writeStream.foreachBatch(
            make_clickhouse_sink(
                "vehicle_latest_position",
                ["vehicle_id", "route_id", "event_time", "lat", "lon", "speed_mps", "bearing_deg", "h3_r9", "h3_r8"],
            )
        )
        .option("checkpointLocation", f"{config.checkpoint_location}/vehicle_latest_position")
        .trigger(processingTime=config.trigger_interval)
        .outputMode("update")
        .start()
    )

    density_query = (
        windowed_density.select(
            "window_start", "window_end", "h3_r9", "h3_r8", "vehicle_count",
            "avg_speed_mps", "max_speed_mps", "sample_count",
        )
        .writeStream.foreachBatch(
            make_clickhouse_sink(
                "h3_density_5min",
                ["window_start", "window_end", "h3_r9", "h3_r8", "vehicle_count",
                 "avg_speed_mps", "max_speed_mps", "sample_count"],
            )
        )
        .option("checkpointLocation", f"{config.checkpoint_location}/h3_density_5min")
        .outputMode("update")
        .trigger(processingTime=config.trigger_interval)
        .start()
    )

    anomaly_query = (
        anomalies.writeStream.foreachBatch(
            make_clickhouse_sink(
                "speed_anomalies",
                ["alert_time", "vehicle_id", "route_id", "h3_r9", "h3_r8", "lat", "lon",
                 "speed_mps", "window_mean_speed_mps", "window_stddev_speed", "z_score",
                 "window_sample_count", "severity"],
            )
        )
        .option("checkpointLocation", f"{config.checkpoint_location}/speed_anomalies")
        .outputMode("append")
        .trigger(processingTime=config.trigger_interval)
        .start()
    )

    log.info("streaming_job.started", trigger_interval=config.trigger_interval)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
