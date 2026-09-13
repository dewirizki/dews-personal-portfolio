"""Canonical event schema every ingestion source normalizes into.

See docs/ARCHITECTURE.md#canonical-event-schema for the design rationale.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field, field_validator


class VehiclePositionEvent(BaseModel):
    vehicle_id: str
    route_id: str = ""
    trip_id: str = ""
    source: Literal["gtfs_rt", "nyc_tlc"]
    lat: float = Field(ge=-90.0, le=90.0)
    lon: float = Field(ge=-180.0, le=180.0)
    speed_mps: float = Field(ge=0.0, le=90.0)  # 90 m/s ≈ 324 km/h, a generous physical ceiling
    bearing_deg: float = Field(default=0.0, ge=0.0, lt=360.0)
    event_time: datetime

    @field_validator("event_time")
    @classmethod
    def ensure_utc(cls, v: datetime) -> datetime:
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)

    def kafka_key(self) -> bytes:
        """Partition key: vehicle_id keeps all events for one vehicle in
        order on one partition, which the anomaly detector's per-vehicle
        continuity relies on."""
        return self.vehicle_id.encode("utf-8")

    def to_json_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")
