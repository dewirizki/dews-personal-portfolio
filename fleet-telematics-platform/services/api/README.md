# services/api

FastAPI query layer over ClickHouse. Three endpoints, each mapping directly
onto one of the example queries in
[`infra/clickhouse/init/01_schema.sql`](../../infra/clickhouse/init/01_schema.sql):

| Endpoint | Backs |
|---|---|
| `GET /density?lat=&lon=&rings=&minutes=` | Viewport density heatmap (H3 k-ring expansion) |
| `GET /anomalies?lat=&lon=&minutes=` | Live speed-anomaly alert feed near a point |
| `GET /vehicles/{vehicle_id}` | Latest known position of one vehicle |

## Run

```bash
cd services/api
poetry install
poetry run run-api        # http://localhost:8000/docs for interactive OpenAPI docs
```
