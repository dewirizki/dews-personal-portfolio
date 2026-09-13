"""Job configuration, overridable via environment variables so the same job
jar/wheel runs unchanged across local Docker Compose, staging, and prod
Kafka/ClickHouse endpoints.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class StreamingConfig:
    kafka_bootstrap_servers: str = os.environ.get("FLEET_KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    raw_positions_topic: str = os.environ.get("FLEET_RAW_POSITIONS_TOPIC", "fleet.vehicle_positions.raw")

    clickhouse_host: str = os.environ.get("FLEET_CLICKHOUSE_HOST", "localhost")
    clickhouse_port: int = int(os.environ.get("FLEET_CLICKHOUSE_PORT", "8123"))
    clickhouse_database: str = os.environ.get("FLEET_CLICKHOUSE_DATABASE", "fleet_telematics")
    clickhouse_user: str = os.environ.get("FLEET_CLICKHOUSE_USER", "default")
    clickhouse_password: str = os.environ.get("FLEET_CLICKHOUSE_PASSWORD", "")

    # H3 resolutions -- see docs/ARCHITECTURE.md §4 for why these two.
    h3_resolution_fine: int = int(os.environ.get("FLEET_H3_RES_FINE", "9"))   # block-level
    h3_resolution_coarse: int = int(os.environ.get("FLEET_H3_RES_COARSE", "8"))  # neighborhood-level

    # Windowing -- see docs/ARCHITECTURE.md §5.
    window_duration: str = os.environ.get("FLEET_WINDOW_DURATION", "5 minutes")
    window_slide: str = os.environ.get("FLEET_WINDOW_SLIDE", "1 minute")
    watermark_delay: str = os.environ.get("FLEET_WATERMARK_DELAY", "2 minutes")
    trigger_interval: str = os.environ.get("FLEET_TRIGGER_INTERVAL", "30 seconds")

    # Anomaly detection thresholds -- see docs/ARCHITECTURE.md §5.
    anomaly_z_warning: float = float(os.environ.get("FLEET_ANOMALY_Z_WARNING", "3.0"))
    anomaly_z_critical: float = float(os.environ.get("FLEET_ANOMALY_Z_CRITICAL", "5.0"))
    anomaly_min_sample_count: int = int(os.environ.get("FLEET_ANOMALY_MIN_SAMPLES", "5"))

    checkpoint_location: str = os.environ.get(
        "FLEET_CHECKPOINT_LOCATION", "/tmp/fleet-telematics/checkpoints"
    )


config = StreamingConfig()
