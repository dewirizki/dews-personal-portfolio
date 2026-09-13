# Fleet Telematics & Spatial Intelligence Platform

A real-time IoT fleet telematics pipeline: live vehicle GPS → Kafka/Redpanda
→ PySpark Structured Streaming (H3 spatial indexing + rolling density +
speed-anomaly detection) → ClickHouse → FastAPI → Streamlit/Grafana.

Built as a portfolio reference implementation of the architecture a real
fleet-ops / logistics data platform runs in production. Full design
rationale lives in [`docs/PRD.md`](docs/PRD.md) and
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — start there.

## Monorepo layout

```
fleet-telematics-platform/
├── docs/                     PRD, architecture, diagrams
├── infra/                    docker-compose (Redpanda, ClickHouse, Grafana) + ClickHouse DDL
├── services/
│   ├── ingestion/            GTFS-Realtime + NYC TLC → normalized events → Kafka
│   ├── stream_processing/    PySpark (primary) + PyFlink (reference) streaming jobs
│   ├── api/                  FastAPI query layer over ClickHouse
│   └── dashboard/            Streamlit live density heatmap + anomaly feed
└── scripts/                  one-off utilities (sample visualization generation, etc.)
```

Each service under `services/` is an **independent Poetry project** with its
own `pyproject.toml` and lockfile — a Spark job's dependency footprint has no
business in the API container's image, and each service should be
buildable/deployable on its own. This repo uses that per-service pattern
rather than a single workspace `pyproject.toml`, since Poetry (as of the
version used here) has no native multi-package workspace mode; a root
[`pyproject.toml`](pyproject.toml) exists only to hold shared dev tooling
(ruff, mypy, pytest, pre-commit) and requires **Poetry ≥ 1.8** for
`package-mode = false`. Each service's own `pyproject.toml` targets Poetry
≥ 1.4 and has been validated standalone.

## Quickstart

```bash
# 1. Bring up the event bus, OLAP store, and Grafana
cd infra && docker compose up -d && cd ..

# 2. Start an ingestion producer (falls back to a bundled fixture if the
#    live feed is unreachable, so this works offline too)
cd services/ingestion && poetry install && poetry run run-gtfs-rt-producer &

# 3. Start the streaming job
cd services/stream_processing && poetry install && poetry run run-streaming-job &

# 4. Start the API + dashboard
cd services/api && poetry install && poetry run run-api &
cd services/dashboard && poetry install && poetry run streamlit run dashboard/app.py
```

Redpanda Console: http://localhost:8080 · ClickHouse HTTP: http://localhost:8123
API docs: http://localhost:8000/docs · Grafana: http://localhost:3000

## What each deliverable demonstrates

| Ask | Where |
|---|---|
| Architecture & tech stack plan | [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) |
| PRD | [`docs/PRD.md`](docs/PRD.md) |
| Streaming PySpark/Flink code: H3 indexing + 5-min rolling density + speed-anomaly alerts | [`services/stream_processing/stream_processing/streaming_job.py`](services/stream_processing/stream_processing/streaming_job.py) (PySpark, primary), [`flink_job.py`](services/stream_processing/stream_processing/flink_job.py) (PyFlink, reference) |
| ClickHouse data model | [`infra/clickhouse/init/01_schema.sql`](infra/clickhouse/init/01_schema.sql) |

A write-up with architecture diagram and sample output is also published on
[my portfolio site](../index.html#fleet-telematics).
