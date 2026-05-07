# ARGOS

> ML forecasting platform for the Greek real estate market: macroeconomic indicators, time-series modeling, and price index predictions.

[![Python](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Status](https://img.shields.io/badge/status-alpha-orange.svg)]()
[![CI](https://github.com/Panagiotis-Andrianopoulos/argos/actions/workflows/ci.yml/badge.svg)](https://github.com/Panagiotis-Andrianopoulos/argos/actions/workflows/ci.yml)

---

## The problem

The Greek real estate market is sensitive to macroeconomic forces — interest rates, inflation, mortgage credit availability, and broader European trends — yet there is no accessible, open platform that ingests these signals and forecasts where the market is heading.

**ARGOS** ingests official price indices and economic indicators from public data sources (FRED, Bank of Greece, Eurostat), engineers features from their relationships, and trains time-series models to predict the next quarterly Residential Property Price Index for Greece.

## What it does

- **Ingests** quarterly economic indicators from FRED (BIS-sourced Greek property indices) and the ECB Data Portal (Greek property indices from BIS, Bank of Greece, and Eurostat aggregates), with Eurostat direct ingestion for EU-wide context planned
- **Archives** raw API responses to S3-compatible object storage for replay and provenance
- **Persists** typed observations to Postgres with idempotent upserts
- **Orchestrates** the pipeline via Apache Airflow with monthly schedules and exponential-backoff retries
- **Trains** time-series models (planned) on the unified dataset to forecast the next quarterly HPI value
- **Serves** predictions via a REST API and an interactive dashboard (planned)

## Architecture

_High-level data flow: ingestion → storage → transformation → ML → serving._

### Data layer

ARGOS persists time-series observations to a **provider-agnostic** schema. Rather than separate tables per source, all upstream data lands in two unified tables:

- **`economic_series`** — one row per series, keyed on `(source, series_id)`. Core columns capture concepts every provider exposes (title, frequency, units, seasonal adjustment, last-updated timestamp). Provider-specific fields are preserved in a JSONB `extra_metadata` column.
- **`economic_observations`** — one row per `(source, series_id, observation_date)`. Holds the value, an optional status (e.g. `provisional` for SDMX sources), and JSONB metadata for revision/vintage information.

Adding a new data source means: adding one enum value to `DataSource`, writing source-specific Pydantic models for the API response, writing a thin persistence module that maps those models to the unified tables, and an Airflow DAG that orchestrates the fetch-archive-persist flow. No schema migrations needed.

## Tech stack

| Layer | Technology |
|---|---|
| Language | Python 3.12 |
| Package manager | uv |
| Data validation | Pydantic |
| HTTP client | httpx, tenacity |
| Object storage | MinIO (local) / Cloudflare R2 (production) |
| Database | PostgreSQL 16 with SQLAlchemy 2.x ORM |
| Migrations | Alembic |
| Orchestration | Apache Airflow 3.2 |
| Testing | pytest, pytest-cov |
| Code quality | ruff, mypy (strict), pre-commit |
| ML & forecasting | scikit-learn, XGBoost (planned) |
| Experiment tracking | MLflow (planned) |
| Serving | FastAPI, Streamlit (planned) |
| Deployment | Docker Compose (local), Fly.io (planned) |
| CI/CD | GitHub Actions |

## Getting started

> **Note:** ARGOS is in early development. The steps below will expand as the project grows.

### Prerequisites

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager
- Docker Desktop
- Git

### Local setup

```bash
# Clone the repository
git clone https://github.com/Panagiotis-Andrianopoulos/argos.git
cd argos

# Install dependencies and set up the virtual environment
uv sync

# Activate the environment
# On Windows (Command Prompt):
.\.venv\Scripts\activate.bat
# On macOS/Linux:
source .venv/bin/activate

# Verify the installation
python -c "import argos; print(argos.__version__)"
```

### Run the local stack

```bash
# Start the data infrastructure (Postgres, MinIO, MLflow, Airflow)
docker compose up -d

# Apply database migrations
uv run alembic upgrade head

# Run the test suite
uv run pytest

# Open the Airflow UI
# http://localhost:8080  (admin / admin)
```

## Project status

**Current milestone:** Data ingestion infrastructure

### Completed
- [x] Project scaffold and `src/` layout
- [x] Code quality tooling (ruff, mypy strict, pre-commit)
- [x] Type-safe configuration with pydantic-settings
- [x] Docker Compose stack (Postgres, MinIO, MLflow, Airflow)
- [x] Database schema with Alembic migrations
- [x] CI/CD with GitHub Actions (lint, migrations, integration tests)
- [x] FRED API client with retry logic and Pydantic validation
- [x] S3-compatible object storage layer with date-partitioned snapshots
- [x] Postgres persistence with idempotent upserts (ON CONFLICT DO UPDATE)
- [x] Provider-agnostic schema (`economic_series`, `economic_observations`) with JSONB metadata for source-specific fields
- [x] Apache Airflow 3.2 orchestration with scheduled monthly DAG
- [x] FRED ingestion: BIS-sourced Greek residential property price index, monthly cadence
- [x] ECB Data Portal ingestion: SDMX REST API with CSV parsing, Greek RESR series ingested from 2006 to present
- [x] 94 tests (unit + integration) running against real Postgres in CI

### In progress
- [ ] Eurostat ingestion for EU-wide context (HICP, mortgage rates, construction costs)

### Planned
- [ ] dbt transformations to feature-engineered datasets
- [ ] Time-series forecasting models with MLflow tracking
- [ ] FastAPI prediction endpoint
- [ ] Streamlit dashboard with historical view and forecast visualization
- [ ] Fly.io deployment

## License

MIT — see [LICENSE](LICENSE) for details.

## Author

**Panagiotis Andrianopoulos** — [GitHub](https://github.com/Panagiotis-Andrianopoulos) · [LinkedIn](https://www.linkedin.com/in/panagiotis-andrianopoulos-62baa830b)
