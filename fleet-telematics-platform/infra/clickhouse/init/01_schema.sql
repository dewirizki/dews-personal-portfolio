-- ============================================================================
-- Fleet Telematics — ClickHouse schema
-- Optimized for: high-throughput micro-batch inserts from Spark/Flink, and
-- low-latency spatial + time-range reads from the API/dashboard layer.
--
-- H3 cells are stored as UInt64 (ClickHouse's native H3 function return type)
-- so geoToH3/h3kRing/h3ToParent etc. can be used directly at query time
-- without any string<->int conversion.
-- ============================================================================

CREATE DATABASE IF NOT EXISTS fleet_telematics;

-- ----------------------------------------------------------------------------
-- 1. raw_vehicle_positions
-- Append-only event store. ORDER BY leads with the spatial key (h3_r8, h3_r9)
-- ahead of event_time: the dominant query pattern is "positions in this
-- neighborhood/cell over the last N minutes", so cells are stored contiguously
-- and a viewport query reads one narrow range instead of scanning partitions.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fleet_telematics.raw_vehicle_positions
(
    event_time       DateTime64(3)             CODEC(Delta, ZSTD),
    ingestion_time    DateTime64(3) DEFAULT now64(3),
    vehicle_id        LowCardinality(String),
    route_id          LowCardinality(String),
    trip_id           String,
    source            LowCardinality(String),   -- 'gtfs_rt' | 'nyc_tlc'
    lat               Float64,
    lon               Float64,
    speed_mps         Float32,
    bearing_deg       Float32,
    h3_r9             UInt64,                   -- resolution 9, ~block-level
    h3_r8             UInt64,                   -- resolution 8, ~neighborhood-level (parent of h3_r9)

    INDEX idx_vehicle_id vehicle_id TYPE bloom_filter GRANULARITY 4,
    INDEX idx_route_id route_id TYPE bloom_filter GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (h3_r8, h3_r9, event_time)
TTL toDateTime(event_time) + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------------------
-- 2. h3_density_5min
-- Pre-aggregated rollup, written directly by the Spark/Flink foreachBatch
-- sink so the streaming job's windowed aggregation is the source of truth
-- for what dispatchers see -- ClickHouse is the serving layer, not the
-- place that (re)computes the window.
--
-- Spark's windowed aggregation runs in `update` output mode: each trigger
-- re-emits the *complete* current count/avg/max for every window still
-- inside the watermark, not a delta. ReplacingMergeTree keyed on
-- (h3_r9, window_start) with `ingested_at` as the version column is the
-- right fit for that: each new trigger's row for a window simply
-- supersedes the previous one on merge -- no separate state-merge logic
-- needed on the ClickHouse side.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fleet_telematics.h3_density_5min
(
    window_start      DateTime64(3),
    window_end        DateTime64(3),
    h3_r9             UInt64,
    h3_r8             UInt64,
    vehicle_count     UInt32,   -- approx_count_distinct(vehicle_id) over the window -- see ARCHITECTURE.md §5
    avg_speed_mps     Float32,
    max_speed_mps     Float32,
    sample_count      UInt32,   -- raw ping count in the window (denominator for anomaly min-sample check)
    ingested_at       DateTime64(3) DEFAULT now64(3)  -- ReplacingMergeTree version: latest trigger wins
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (h3_r9, window_start)
TTL toDateTime(window_start) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- Query with FINAL (or through this view) for read-your-writes correctness
-- between background merges -- see services/api for how the density
-- endpoint uses it.
CREATE VIEW IF NOT EXISTS fleet_telematics.h3_density_5min_current AS
SELECT window_start, window_end, h3_r9, h3_r8, vehicle_count, avg_speed_mps, max_speed_mps, sample_count
FROM fleet_telematics.h3_density_5min FINAL;

-- Independent validation path: recomputes the same 5-minute density
-- directly from raw_vehicle_positions using ClickHouse-native aggregation.
-- Run ad hoc (not continuously materialized) to reconcile against the
-- Spark-computed rollup above -- see PRD §7, "Success metrics / validation".
CREATE VIEW IF NOT EXISTS fleet_telematics.h3_density_5min_validation AS
SELECT
    toStartOfFiveMinute(event_time)                   AS window_start,
    toStartOfFiveMinute(event_time) + INTERVAL 5 MINUTE AS window_end,
    h3_r9,
    h3_r8,
    uniq(vehicle_id)      AS vehicle_count,
    avg(speed_mps)        AS avg_speed_mps,
    max(speed_mps)        AS max_speed_mps,
    count()               AS sample_count
FROM fleet_telematics.raw_vehicle_positions
GROUP BY window_start, window_end, h3_r9, h3_r8;

-- ----------------------------------------------------------------------------
-- 3. speed_anomalies
-- Alert feed written by the stream-stream join anomaly detector. Narrow,
-- short-TTL, ordered for "recent alerts near me" and "alert history for
-- this cell" queries alike.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fleet_telematics.speed_anomalies
(
    alert_time            DateTime64(3),
    vehicle_id            LowCardinality(String),
    route_id              LowCardinality(String),
    h3_r9                 UInt64,
    h3_r8                 UInt64,
    lat                   Float64,
    lon                   Float64,
    speed_mps             Float32,
    window_mean_speed_mps Float32,
    window_stddev_speed   Float32,
    z_score               Float32,
    window_sample_count   UInt32,
    severity              LowCardinality(String)  -- 'WARNING' (|z|>=3) | 'CRITICAL' (|z|>=5)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(alert_time)
ORDER BY (h3_r9, alert_time)
TTL toDateTime(alert_time) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- ----------------------------------------------------------------------------
-- 4. vehicle_latest_position
-- Upserted (via ReplacingMergeTree, versioned on event_time) by the streaming
-- job's foreachBatch sink. Gives an O(primary-key-lookup) answer to
-- "where is vehicle X right now" without scanning raw_vehicle_positions.
-- Query with `FINAL` or through the _current view below for correctness
-- between background merges.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fleet_telematics.vehicle_latest_position
(
    vehicle_id   String,
    route_id     LowCardinality(String),
    event_time   DateTime64(3),
    lat          Float64,
    lon          Float64,
    speed_mps    Float32,
    bearing_deg  Float32,
    h3_r9        UInt64,
    h3_r8        UInt64
)
ENGINE = ReplacingMergeTree(event_time)
ORDER BY vehicle_id;

CREATE VIEW IF NOT EXISTS fleet_telematics.vehicle_latest_position_current AS
SELECT * FROM fleet_telematics.vehicle_latest_position FINAL;

-- ----------------------------------------------------------------------------
-- Example low-latency queries the schema above is shaped for
-- (referenced by services/api — kept here for documentation/EXPLAIN use)
-- ----------------------------------------------------------------------------

-- a) "Density heatmap for the current viewport, last 5 minutes" --
--    h3kRing expands a center cell to its k-ring neighbors at query time;
--    indexing was already done at write time, so this is a pure lookup.
-- SELECT h3_r9, vehicle_count, avg_speed_mps
-- FROM fleet_telematics.h3_density_5min_current
-- WHERE h3_r9 IN (h3kRing({center_h3:UInt64}, {rings:UInt8}))
--   AND window_start >= now() - INTERVAL 5 MINUTE
-- ORDER BY window_start DESC;

-- b) "Live alert feed near me" --
-- SELECT * FROM fleet_telematics.speed_anomalies
-- WHERE h3_r8 = h3ToParent({center_h3:UInt64}, 8)
--   AND alert_time >= now() - INTERVAL 15 MINUTE
-- ORDER BY alert_time DESC
-- LIMIT 200;

-- c) "Where is vehicle X" --
-- SELECT * FROM fleet_telematics.vehicle_latest_position_current
-- WHERE vehicle_id = {vehicle_id:String};
