# services/dashboard

Streamlit dashboard: a live H3 hexagon density heatmap (via `pydeck`) and a
scrolling speed-anomaly alert feed. Reads exclusively through
[`services/api`](../api) — never touches ClickHouse directly.

## Run

```bash
cd services/dashboard
poetry install
FLEET_API_BASE_URL=http://localhost:8000 poetry run streamlit run dashboard/app.py
```
