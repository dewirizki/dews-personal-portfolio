"""Thin, typed query layer over ClickHouse.

Every endpoint maps directly onto one of the example queries documented in
infra/clickhouse/init/01_schema.sql -- this module intentionally does not
add business logic ClickHouse can express itself; it exists to give the
dashboard/Grafana a stable, parameterized HTTP contract instead of raw SQL
access.
"""

from __future__ import annotations

import h3
from fastapi import FastAPI, HTTPException, Query

from api.clickhouse_client import get_client

app = FastAPI(
    title="Fleet Telematics API",
    description="Query layer over ClickHouse for H3 density, speed anomalies, and vehicle lookups.",
    version="0.1.0",
)


@app.get("/health")
def health() -> dict:
    get_client().command("SELECT 1")
    return {"status": "ok"}


@app.get("/density")
def density(
    lat: float = Query(..., ge=-90, le=90, description="Viewport center latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Viewport center longitude"),
    rings: int = Query(6, ge=0, le=30, description="H3 k-ring radius around the center cell"),
    resolution: int = Query(9, ge=0, le=15),
    minutes: int = Query(5, ge=1, le=180, description="Look back this many minutes"),
) -> list[dict]:
    """Density heatmap for the current viewport -- see
    infra/clickhouse/init/01_schema.sql, example query (a)."""
    center_cell = h3.str_to_int(h3.latlng_to_cell(lat, lon, resolution))

    result = get_client().query(
        """
        SELECT h3_r9, window_start, vehicle_count, avg_speed_mps, max_speed_mps
        FROM fleet_telematics.h3_density_5min_current
        WHERE h3_r9 IN (h3kRing({center_h3:UInt64}, {rings:UInt8}))
          AND window_start >= now() - INTERVAL {minutes:UInt16} MINUTE
        ORDER BY window_start DESC
        """,
        parameters={"center_h3": center_cell, "rings": rings, "minutes": minutes},
    )
    return [dict(zip(result.column_names, row)) for row in result.result_rows]


@app.get("/anomalies")
def anomalies(
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    minutes: int = Query(15, ge=1, le=180),
    limit: int = Query(200, ge=1, le=1000),
) -> list[dict]:
    """Live alert feed near a point -- see
    infra/clickhouse/init/01_schema.sql, example query (b)."""
    center_h3_r8 = h3.str_to_int(h3.latlng_to_cell(lat, lon, 8))

    result = get_client().query(
        """
        SELECT alert_time, vehicle_id, route_id, lat, lon, speed_mps,
               window_mean_speed_mps, z_score, severity
        FROM fleet_telematics.speed_anomalies
        WHERE h3_r8 = {center_h3_r8:UInt64}
          AND alert_time >= now() - INTERVAL {minutes:UInt16} MINUTE
        ORDER BY alert_time DESC
        LIMIT {limit:UInt16}
        """,
        parameters={"center_h3_r8": center_h3_r8, "minutes": minutes, "limit": limit},
    )
    return [dict(zip(result.column_names, row)) for row in result.result_rows]


@app.get("/vehicles/{vehicle_id}")
def vehicle_latest_position(vehicle_id: str) -> dict:
    """Where is vehicle X right now -- see
    infra/clickhouse/init/01_schema.sql, example query (c)."""
    result = get_client().query(
        """
        SELECT vehicle_id, route_id, event_time, lat, lon, speed_mps, bearing_deg
        FROM fleet_telematics.vehicle_latest_position_current
        WHERE vehicle_id = {vehicle_id:String}
        """,
        parameters={"vehicle_id": vehicle_id},
    )
    if not result.result_rows:
        raise HTTPException(status_code=404, detail=f"No known position for vehicle {vehicle_id!r}")
    return dict(zip(result.column_names, result.result_rows[0]))


def run() -> None:
    import uvicorn

    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    run()
