# PRD — Real-Time Fleet Telematics & Spatial Intelligence Platform

**Author:** Dewi Rizki Fitriani · **Status:** v1.0 (portfolio reference implementation) · **Last updated:** 2026-09-13

## 1. Problem statement

Fleet operators (transit agencies, logistics carriers, ride-hail networks) receive a firehose of
GPS pings from thousands of vehicles but rarely turn that stream into two things dispatchers
actually need in under a few seconds:

1. **Where is demand/supply densest right now**, at a spatial resolution fine enough to act on
   (city-block, not city-wide) — for rebalancing vehicles, routing new pickups, or flagging
   congestion.
2. **Which vehicles are behaving abnormally right now** — GPS jumps, harsh braking/acceleration,
   or a vehicle far outside the speed profile of its surrounding traffic — before it becomes a
   safety incident or a data-quality problem downstream.

Most public reference architectures stop at "ingest GPS into a database." This project builds the
full path: **live public telemetry → stream processing with spatial indexing → an OLAP store shaped
for sub-second spatial queries → an operational dashboard**, using the same architectural pattern a
real fleet/logistics data platform would run in production.

## 2. Goals

| # | Goal | Success metric |
|---|------|-----------------|
| G1 | Ingest live, public vehicle-position telemetry continuously | ≥ 99% of polling cycles succeed against the upstream feed |
| G2 | Index every position to an H3 cell at two resolutions in the streaming layer | H3 assignment added with < 1 partition-local shuffle per micro-batch |
| G3 | Compute rolling 5-minute vehicle density per H3 cell | End-to-end latency (GPS ping → row queryable in ClickHouse) < 30s p95 |
| G4 | Detect speed anomalies relative to local (same H3 cell, same window) traffic, not a fixed threshold | Anomaly precision validated by manual spot-check on ≥ 50 flagged events |
| G5 | Serve spatial + time-range queries fast enough for an interactive map | p95 query latency < 200ms for a "last 5 minutes, current viewport" query |
| G6 | Reproducible by a reviewer | `docker compose up` + one job submission brings the full stack up locally |

### Non-goals

- Production-grade multi-tenant security, autoscaling, or SLA guarantees.
- Building a routing/dispatch optimizer — this platform produces the *signals* a dispatch system
  would consume, not the dispatch decision itself.
- Exhaustive coverage of every GTFS-RT or TLC field — only the fields needed for spatial density
  and speed-anomaly use cases are modeled.

## 3. Data sources

| Source | What it provides | Update cadence | Notes |
|---|---|---|---|
| [Transitland / GTFS-Realtime `VehiclePosition`](https://gtfs.org/realtime/) feeds | `vehicle_id`, `trip_id`, `route_id`, lat/lon, bearing, speed, timestamp | Typically 10–30s per feed | Protobuf; one feed per transit agency, selectable via Transitland's feed registry |
| [NYC TLC trip record / high-volume FHV feeds](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) | Pickup/drop-off geocoordinates, timestamps, trip distance | Batch (historical) — replayed at simulated real-time cadence for the streaming demo | Used as a second, structurally different source to prove the ingestion layer is source-agnostic |

Both sources are normalized in the ingestion layer to one canonical Avro/JSON schema
(`VehiclePositionEvent`, see [ARCHITECTURE.md](./ARCHITECTURE.md#canonical-event-schema)) before
hitting Kafka/Redpanda — nothing downstream needs to know which source an event came from.

## 4. Users & use cases

- **Fleet dispatcher (primary persona):** watches a live density heatmap to see where vehicles are
  clustering or thinning out, and gets alerted when a vehicle's speed diverges sharply from the
  local traffic pattern around it.
- **Data/analytics engineer (secondary persona, i.e. a reviewer of this portfolio):** reads the
  architecture to evaluate stream-processing design, spatial data modeling, and OLAP schema
  choices.

## 5. Functional requirements

1. **FR1** — Ingestion service polls/streams vehicle positions from at least one live public
   source and publishes normalized events to a Kafka/Redpanda topic (`fleet.vehicle_positions.raw`)
   partitioned by a spatial key so downstream consumers get locality.
2. **FR2** — Stream processing job consumes the raw topic, assigns each event an
   [H3](https://h3geo.org/) cell at **resolution 9** (~0.1 km² hexagons, block-level) and the
   **resolution 8 parent** (~0.7 km², neighborhood-level), and writes enriched events onward.
3. **FR3** — A 5-minute tumbling-with-1-minute-slide window computes, per H3-r9 cell: distinct
   vehicle count, mean speed, and speed standard deviation.
4. **FR4** — A stream-stream join enriches each raw event with the current windowed statistics for
   its H3 cell and flags it as an anomaly when its speed's z-score against the local
   mean/stddev exceeds a configurable threshold (default `|z| ≥ 3`, minimum sample size 5).
5. **FR5** — Density aggregates and anomaly events are persisted to ClickHouse in a schema
   supporting both "heatmap for current viewport" and "alert feed" query patterns.
6. **FR6** — A dashboard (Streamlit, with an optional Grafana board against the same ClickHouse
   tables) renders the live H3 density heatmap and a scrolling anomaly alert feed.

## 6. Non-functional requirements

- **Latency:** GPS ping to ClickHouse-queryable row < 30s p95 (dominated by the ingestion poll
  interval + micro-batch trigger interval, not by ClickHouse itself).
- **Throughput:** design point of 5,000 vehicles reporting every 10s (~500 events/s sustained) —
  matches a mid-size metro transit agency's live fleet.
- **Query latency:** p95 < 200ms for "current density in viewport + last N minutes."
- **Data retention:** raw positions 7 days, aggregated density 90 days, anomaly alerts 30 days —
  chosen to bound storage while keeping enough history for before/after incident review.
- **Reproducibility:** entire stack runs on a single developer laptop via Docker Compose.

## 7. Success metrics / validation

- Replay a fixed historical window through the pipeline and confirm density totals reconcile with
  a batch (Pandas) recomputation — this is the correctness backstop for the streaming aggregation.
- Manually review a sample of flagged anomalies against the raw trace to sanity-check the z-score
  threshold (documented in `docs/ARCHITECTURE.md` under "anomaly detection").

## 8. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Public feed goes down or rate-limits | Ingestion falls back to a bundled recorded sample (`fixtures/sample_vehicle_positions.jsonl`) replayed at real-time cadence so the rest of the pipeline still demos end to end |
| H3 resolution choice doesn't match vehicle density (too sparse/too crowded per cell) | Both r8 and r9 are computed so the aggregation resolution is a query-time choice, not a re-processing job |
| Stream-stream join state growth unbounded | Both sides of the join are watermarked; state is dropped once past the watermark, bounding memory |
| Single ClickHouse node latency under bursty writes | Batched `foreachBatch` inserts (not row-by-row) sized to the micro-batch interval |

## 9. Out of scope for v1

Kubernetes deployment manifests, multi-region replication, authn/authz on the API layer, and a
production alerting integration (PagerDuty/Slack) — the alert *table* exists; wiring it to a
notification channel is a natural v2 extension, noted in `docs/ARCHITECTURE.md`.
