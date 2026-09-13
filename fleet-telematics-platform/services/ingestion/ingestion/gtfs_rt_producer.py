"""Polls a GTFS-Realtime VehiclePositions feed (e.g. any agency registered on
Transitland: https://www.transitland.org/feeds) and publishes normalized
VehiclePositionEvents to Kafka/Redpanda.

Run:
    poetry run run-gtfs-rt-producer

Point at a different agency feed with FLEET_GTFS_RT_FEED_URL.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
import structlog
from google.transit import gtfs_realtime_pb2
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from ingestion.config import settings
from ingestion.producer import build_producer, publish
from ingestion.schemas import VehiclePositionEvent

log = structlog.get_logger()


@retry(
    retry=retry_if_exception_type(httpx.HTTPError),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(settings.max_retry_attempts),
)
def fetch_feed(client: httpx.Client) -> bytes:
    response = client.get(settings.gtfs_rt_feed_url, timeout=settings.request_timeout_seconds)
    response.raise_for_status()
    return response.content


def parse_vehicle_positions(payload: bytes) -> list[VehiclePositionEvent]:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)

    events: list[VehiclePositionEvent] = []
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        vp = entity.vehicle
        if not vp.HasField("position"):
            continue

        event_time = (
            datetime.fromtimestamp(vp.timestamp, tz=timezone.utc)
            if vp.timestamp
            else datetime.now(tz=timezone.utc)
        )
        try:
            events.append(
                VehiclePositionEvent(
                    vehicle_id=vp.vehicle.id or entity.id,
                    route_id=vp.trip.route_id,
                    trip_id=vp.trip.trip_id,
                    source="gtfs_rt",
                    lat=vp.position.latitude,
                    lon=vp.position.longitude,
                    speed_mps=max(vp.position.speed, 0.0) if vp.position.HasField("speed") else 0.0,
                    bearing_deg=vp.position.bearing if vp.position.HasField("bearing") else 0.0,
                    event_time=event_time,
                )
            )
        except ValueError as exc:
            # Malformed upstream record (out-of-range lat/lon/speed) -- skip
            # and count, don't crash the poll loop over one bad entity.
            log.warning("gtfs_rt.invalid_record", entity_id=entity.id, error=str(exc))

    return events


def replay_fallback_fixture(producer, topic: str) -> None:  # type: ignore[no-untyped-def]
    """Used when the live feed is unreachable after all retries -- keeps the
    downstream pipeline demoable offline. See PRD.md, Risks & mitigations."""
    fixture_path = Path(settings.fallback_fixture_path)
    if not fixture_path.exists():
        log.error("gtfs_rt.fallback_missing", path=str(fixture_path))
        return

    log.warning("gtfs_rt.using_fallback_fixture", path=str(fixture_path))
    with fixture_path.open() as f:
        for line in f:
            event = VehiclePositionEvent.model_validate_json(line)
            event = event.model_copy(update={"event_time": datetime.now(tz=timezone.utc)})
            publish(producer, event, topic)
            time.sleep(1.0 / max(settings.gtfs_rt_poll_interval_seconds, 1))


def run() -> None:
    producer = build_producer()
    log.info(
        "gtfs_rt.starting",
        feed_url=settings.gtfs_rt_feed_url,
        topic=settings.raw_positions_topic,
        poll_interval_s=settings.gtfs_rt_poll_interval_seconds,
    )

    with httpx.Client() as client:
        while True:
            try:
                payload = fetch_feed(client)
                events = parse_vehicle_positions(payload)
                for event in events:
                    publish(producer, event, settings.raw_positions_topic)
                producer.flush(5.0)
                log.info("gtfs_rt.batch_published", count=len(events))
            except httpx.HTTPError as exc:
                log.error("gtfs_rt.feed_unreachable", error=str(exc))
                replay_fallback_fixture(producer, settings.raw_positions_topic)

            time.sleep(settings.gtfs_rt_poll_interval_seconds)


def main() -> None:
    run()


if __name__ == "__main__":
    main()
