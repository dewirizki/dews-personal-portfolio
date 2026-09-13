"""Replays a NYC TLC high-volume FHV trip-record extract at a simulated
real-time cadence, publishing normalized VehiclePositionEvents.

NYC TLC trip records (https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
are published as monthly batch files, not a live stream. Replaying one at a
controlled events/sec rate lets this pipeline prove the ingestion contract is
source-agnostic: a structurally different source (batch pickup/drop-off pairs
vs. a live GPS ping stream) normalizes to the same VehiclePositionEvent and
flows through the identical Kafka topic and downstream jobs.

Each trip's pickup -> dropoff pair is linearly interpolated into a short
sequence of position pings so the stream-processing layer sees continuous
vehicle motion, not two isolated points.

Run:
    poetry run run-nyc-tlc-producer
"""

from __future__ import annotations

import math
import time
import uuid
from datetime import datetime, timedelta, timezone

import pandas as pd
import structlog

from ingestion.config import settings
from ingestion.producer import build_producer, publish
from ingestion.schemas import VehiclePositionEvent

log = structlog.get_logger()

# Interpolation resolution: how many GPS pings to synthesize per trip.
PINGS_PER_TRIP = 6


def _haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def load_trips(source_path: str) -> pd.DataFrame:
    df = pd.read_parquet(source_path)
    required = {
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_latitude",
        "pickup_longitude",
        "dropoff_latitude",
        "dropoff_longitude",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"NYC TLC extract missing expected columns: {missing}")
    return df.dropna(subset=list(required))


def trip_to_events(row: pd.Series) -> list[VehiclePositionEvent]:
    vehicle_id = f"TLC-{uuid.uuid4().hex[:8]}"
    t0: datetime = row["pickup_datetime"].to_pydatetime().replace(tzinfo=timezone.utc)
    t1: datetime = row["dropoff_datetime"].to_pydatetime().replace(tzinfo=timezone.utc)
    duration_s = max((t1 - t0).total_seconds(), 1.0)
    distance_m = _haversine_meters(
        row["pickup_latitude"], row["pickup_longitude"], row["dropoff_latitude"], row["dropoff_longitude"]
    )
    avg_speed_mps = min(distance_m / duration_s, 45.0)  # clamp: short/noisy trips shouldn't imply highway speed

    events = []
    for i in range(PINGS_PER_TRIP):
        frac = i / (PINGS_PER_TRIP - 1)
        lat = row["pickup_latitude"] + frac * (row["dropoff_latitude"] - row["pickup_latitude"])
        lon = row["pickup_longitude"] + frac * (row["dropoff_longitude"] - row["pickup_longitude"])
        events.append(
            VehiclePositionEvent(
                vehicle_id=vehicle_id,
                route_id="nyc-fhv",
                trip_id=str(row.get("trip_id", uuid.uuid4().hex[:12])),
                source="nyc_tlc",
                lat=lat,
                lon=lon,
                speed_mps=avg_speed_mps,
                bearing_deg=0.0,
                event_time=t0 + timedelta(seconds=frac * duration_s),
            )
        )
    return events


def run() -> None:
    producer = build_producer()
    trips = load_trips(settings.nyc_tlc_source_path)
    log.info("nyc_tlc.starting", trips=len(trips), topic=settings.raw_positions_topic)

    delay_s = 1.0 / max(settings.nyc_tlc_replay_events_per_second, 0.1)
    for _, row in trips.iterrows():
        for event in trip_to_events(row):
            # Replay at "now" cadence rather than the original historical
            # timestamps, so the streaming layer's watermarking sees a live
            # stream -- the point is to exercise real-time semantics, not to
            # do historical backfill.
            replayed_event = event.model_copy(update={"event_time": datetime.now(tz=timezone.utc)})
            publish(producer, replayed_event, settings.raw_positions_topic)
            time.sleep(delay_s)
    producer.flush(10.0)
    log.info("nyc_tlc.replay_complete")


def main() -> None:
    run()


if __name__ == "__main__":
    main()
