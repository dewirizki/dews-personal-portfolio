"""Thin wrapper around confluent_kafka.Producer shared by every source."""

from __future__ import annotations

import structlog
from confluent_kafka import Producer

from ingestion.config import settings
from ingestion.schemas import VehiclePositionEvent

log = structlog.get_logger()


def build_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "linger.ms": 50,  # small batching window: throughput over per-event latency
            "compression.type": "zstd",
            "enable.idempotence": True,
            "acks": "all",
        }
    )


def _delivery_callback(err, msg) -> None:  # type: ignore[no-untyped-def]
    if err is not None:
        log.warning("kafka.delivery_failed", error=str(err))


def publish(producer: Producer, event: VehiclePositionEvent, topic: str) -> None:
    producer.produce(
        topic=topic,
        key=event.kafka_key(),
        value=event.to_json_bytes(),
        callback=_delivery_callback,
    )
    producer.poll(0)  # serve delivery callbacks without blocking the caller
