.PHONY: infra-up infra-down install-all test-all lint-all

infra-up:
	cd infra && docker compose up -d

infra-down:
	cd infra && docker compose down

install-all:
	cd services/ingestion && poetry install
	cd services/stream_processing && poetry install
	cd services/api && poetry install
	cd services/dashboard && poetry install

test-all:
	cd services/stream_processing && poetry run pytest

lint-all:
	cd services/ingestion && poetry run ruff check .
	cd services/stream_processing && poetry run ruff check .
	cd services/api && poetry run ruff check .
	cd services/dashboard && poetry run ruff check .
