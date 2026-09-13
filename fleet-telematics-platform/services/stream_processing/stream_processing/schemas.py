"""Spark schema for the raw Kafka payload -- mirrors
services/ingestion/ingestion/schemas.py::VehiclePositionEvent field for
field. Kept as an explicit StructType (rather than schema inference) so a
malformed upstream message fails a single row via `from_json`'s null
behavior instead of silently reshaping the whole streaming DataFrame.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    FloatType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

VEHICLE_POSITION_SCHEMA = StructType(
    [
        StructField("vehicle_id", StringType(), nullable=False),
        StructField("route_id", StringType(), nullable=True),
        StructField("trip_id", StringType(), nullable=True),
        StructField("source", StringType(), nullable=False),
        StructField("lat", DoubleType(), nullable=False),
        StructField("lon", DoubleType(), nullable=False),
        StructField("speed_mps", FloatType(), nullable=False),
        StructField("bearing_deg", FloatType(), nullable=True),
        StructField("event_time", TimestampType(), nullable=False),
    ]
)
