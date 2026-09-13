"""Shared configuration for all ingestion producers.

Every setting is overridable via environment variables (see `env` in each
field's `Field(...)`), so the same image runs against a laptop Redpanda or a
real Kafka cluster without code changes.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class IngestionSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="FLEET_", env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = Field(default="localhost:19092")
    raw_positions_topic: str = Field(default="fleet.vehicle_positions.raw")

    # GTFS-Realtime: point at any Transitland-registered agency feed URL.
    # Example (MTA Bus Time, no API key required): https://gtfsrt.prod.obanyc.com/vehiclePositions
    gtfs_rt_feed_url: str = Field(
        default="https://gtfsrt.prod.obanyc.com/vehiclePositions",
        description="GTFS-Realtime VehiclePositions protobuf feed URL.",
    )
    gtfs_rt_poll_interval_seconds: float = Field(default=15.0)

    # NYC TLC high-volume FHV trip records are published as monthly Parquet
    # files, not a live stream -- this pipeline replays a local extract at a
    # simulated real-time cadence to exercise the same ingestion contract
    # with a structurally different source (see PRD.md, data sources).
    nyc_tlc_source_path: str = Field(
        default="fixtures/nyc_tlc_sample.parquet",
        description="Local path to a NYC TLC trip-record extract used for replay.",
    )
    nyc_tlc_replay_events_per_second: float = Field(default=20.0)

    # If the live feed is unreachable (offline demo, rate limiting), fall
    # back to a bundled recorded sample so the rest of the pipeline still
    # runs end to end -- see PRD.md, Risks & mitigations.
    fallback_fixture_path: str = Field(default="fixtures/sample_vehicle_positions.jsonl")

    request_timeout_seconds: float = Field(default=10.0)
    max_retry_attempts: int = Field(default=5)


settings = IngestionSettings()
