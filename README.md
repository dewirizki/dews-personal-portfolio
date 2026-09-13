# dews-personal-portfolio
Documenting my journey into data engineering through small projects, experiments, and continuous learning ehehe🙂‍↕️

## What's in here

- `index.html`, `assets/` — the portfolio website (static, no build step).
- `portfolio-blueprint.html` — the working content package: positioning, 7 project case studies, the Canva PDF page-by-page plan, and website copy, all sourced from the CV/competition paper.
- `fleet-telematics-platform/` — independent project: a real-time IoT fleet telematics & spatial intelligence platform, structured as a Poetry monorepo. See below.
- `.github/workflows/deploy.yml` — deploys `index.html` to GitHub Pages on every push to `main`.

## Fleet Telematics Platform

`fleet-telematics-platform/` is a self-directed build, not a work project — I built it to apply my
math and real-time-streaming background to a system shaped like one a fleet-ops or logistics data
platform would actually run in production, rather than a notebook-only demo.

**Goal.** Turn a live firehose of vehicle GPS pings into two things a fleet dispatcher actually
needs within seconds: (1) where vehicle density is clustering right now, at block-level
resolution, and (2) which vehicles are behaving abnormally *relative to the traffic immediately
around them* — not against a fixed, context-blind speed threshold.

**What I did.**

- Normalized two structurally different live/replayed sources — GTFS-Realtime `VehiclePosition`
  feeds and NYC TLC trip records — into one canonical event schema, published to Kafka/Redpanda.
- Wrote a PySpark Structured Streaming job (plus a PyFlink reference implementation, to make the
  engine trade-off concrete rather than asserted) that indexes every event to an
  [Uber H3](https://h3geo.org/) hexagon at resolution 9 (block-level) and its resolution-8 parent,
  computes a 5-minute/1-minute-slide rolling vehicle density per cell, and runs a watermarked
  stream-stream join back onto the raw stream so each event's speed is scored — by z-score —
  against its own H3 cell's current local mean/stddev, not a global threshold.
- Designed a ClickHouse schema optimized for that access pattern: spatial-key-first ordering so a
  viewport+time query reads one contiguous range, a `ReplacingMergeTree` density rollup fed
  directly by Spark's `foreachBatch`, and native H3 functions (`h3kRing`, `h3ToParent`) doing
  ring/neighbor expansion at query time only.
- Wrote a PRD and an architecture doc up front (`docs/PRD.md`, `docs/ARCHITECTURE.md`) — goals,
  non-goals, functional/non-functional requirements, and the reasoning behind every stack choice —
  the way I'd want a system like this scoped before writing the streaming code.
- Split the system into independently Poetry-managed services (`ingestion`, `stream_processing`,
  `api`, `dashboard`) rather than one shared dependency set, and wired up Docker Compose
  (Redpanda + ClickHouse + Grafana) so the whole stack is reproducible on one laptop.

**Tech stack:** Kafka/Redpanda, PySpark Structured Streaming, PyFlink, Uber H3, ClickHouse,
FastAPI, Streamlit, Grafana, Poetry.

Full write-up, architecture diagram, and sample pipeline output: `index.html#fleet-telematics` on
the live site. Start reading the code at
[`docs/PRD.md`](fleet-telematics-platform/docs/PRD.md),
[`docs/ARCHITECTURE.md`](fleet-telematics-platform/docs/ARCHITECTURE.md), and
[`services/stream_processing/stream_processing/streaming_job.py`](fleet-telematics-platform/services/stream_processing/stream_processing/streaming_job.py).

## Deploying

This repo deploys via GitHub Actions + GitHub Pages. One manual step is required once, since it can't be done from the CLI:

1. Push this repo to GitHub (already set up if you're reading this from the repo).
2. On GitHub: **Settings → Pages → Build and deployment → Source → GitHub Actions**.
3. Push to `main` (or re-run the "Deploy portfolio to GitHub Pages" workflow from the Actions tab) — the site publishes to `https://<username>.github.io/dews-personal-portfolio/`.
4. Once ready, point your custom domain (if any) at the Pages site under **Settings → Pages → Custom domain**, and add the matching DNS records at your domain registrar.
