# ARGOS

> Real estate intelligence platform for the Greek market: live listings, objective values, and fair-price forecasting.

[![Python](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Status](https://img.shields.io/badge/status-alpha-orange.svg)]()

---

## The problem

The Greek real estate market lacks transparent, data-driven pricing signals. Buyers and sellers rely on listing prices that rarely reflect fair market value, while official AADE "objective values" lag behind actual market dynamics by years.

**ARGOS** aggregates live listings from multiple sources, cross-references them with official AADE objective values and Bank of Greece indices, and predicts a **fair price** for any property — flagging listings that are significantly over- or underpriced.

## What it does

- **Continuously ingests** listings from Spitogatos, XE.gr, and public data sources (AADE, ELSTAT, Bank of Greece)
- **Normalizes** heterogeneous data through a dbt-modeled warehouse
- **Trains** gradient-boosted models (XGBoost) to predict fair prices
- **Serves** predictions via a REST API and an interactive dashboard
- **Explains** each prediction using SHAP values

## Architecture

_High-level data flow: ingestion → storage → transformation → ML → serving. Full diagram coming soon._

## Tech stack

| Layer | Technology |
|---|---|
| Language | Python 3.12 |
| Package manager | uv |
| Ingestion | Scrapy, Playwright, httpx |
| Orchestration | Apache Airflow |
| Storage | Postgres (Supabase), Cloudflare R2 |
| Transformation | dbt, pandas |
| Machine learning | scikit-learn, XGBoost, Optuna, SHAP |
| Experiment tracking | MLflow |
| Serving | FastAPI, Streamlit |
| Deployment | Docker, Fly.io |
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
git clone https://github.com//argos.git
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

## Project status

**Current milestone:** Foundation (Week 1 of 6)

- [x] Project scaffold
- [x] Package configuration
- [ ] Docker Compose stack
- [ ] Data ingestion layer
- [ ] dbt transformations
- [ ] ML training pipeline
- [ ] FastAPI serving layer
- [ ] Streamlit dashboard
- [ ] Fly.io deployment

## License

MIT — see [LICENSE](LICENSE) for details.

## Author

**Panagiotis Andrianopoulos** — [GitHub](https://github.com/Panagiotis-Andrianopoulos) · [LinkedIn](https://www.linkedin.com/in/panagiotis-andrianopoulos-62baa830b)
