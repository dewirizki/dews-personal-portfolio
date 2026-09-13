from __future__ import annotations

import os
from functools import lru_cache

import clickhouse_connect
from clickhouse_connect.driver.client import Client


@lru_cache(maxsize=1)
def get_client() -> Client:
    return clickhouse_connect.get_client(
        host=os.environ.get("FLEET_CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("FLEET_CLICKHOUSE_PORT", "8123")),
        database=os.environ.get("FLEET_CLICKHOUSE_DATABASE", "fleet_telematics"),
        username=os.environ.get("FLEET_CLICKHOUSE_USER", "default"),
        password=os.environ.get("FLEET_CLICKHOUSE_PASSWORD", ""),
    )
