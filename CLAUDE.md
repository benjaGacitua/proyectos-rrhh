# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

All commands should be run from the `actualizacion-tablas-2/` subdirectory unless noted otherwise.

**Build and run (Docker - recommended):**
```bash
cd actualizacion-tablas-2
docker compose up --build -d        # first run or after dependency changes
docker compose up -d                # subsequent runs
docker compose down                 # stop containers
docker logs rh_cramer_etl           # view output
```

**Run manually (local Python):**
```bash
cd actualizacion-tablas-2
python app/main.py
```

**Scheduled execution via shell script (prevents concurrent runs with flock):**
```bash
cd actualizacion-tablas-2
./run_etl.sh
```

**Reset task state (force re-run of all tasks today):**
Edit or delete `actualizacion-tablas-2/staging/estado_sincronizacion.json`. Each task entry has a date field; clearing or backdating it allows re-execution.

**Airflow mode (orchestrated):**
```bash
cd actualizacion-tablas-2
mkdir -p staging logs          # ensure dirs exist before first run

# First time only — initialize Airflow metadata DB and create admin user:
docker compose -f compose.airflow.yml up airflow-init --build

# Then start scheduler + UI:
docker compose -f compose.airflow.yml up airflow-scheduler airflow-webserver -d

# UI: http://localhost:8080  (admin / admin)
# Trigger a DAG run manually: Airflow UI → DAG "etl_rh_cramer" → Trigger
```
Add `AIRFLOW_SECRET_KEY=<random_string>` to `.env` before first run.

## Architecture

This is a daily ETL pipeline that syncs HR data from the BUK API to a PostgreSQL database via SSH tunnel. It can run in two modes:

- **Standalone** (`compose.yml` + `app/main.py`) — single container, sequential, uses `estado_sincronizacion.json` to skip tasks already done today.
- **Airflow** (`compose.airflow.yml` + `app/dags/etl_rh_dag.py`) — Airflow scheduler drives the same callables; task state is managed by Airflow's metadata DB.

```
BUK HR API
  └─→ extract.py        (HTTP via requests, paginated)
  └─→ staging/          (JSON cache for fault tolerance)
  └─→ load.py           (upsert via psycopg2)
  └─→ PostgreSQL (rh schema, accessed via SSH tunnel)
```

### Key Modules

- **`app/main.py`** — Standalone entry point. Orchestrates 5 sequential ETL blocks: employees, areas, vacations, consolidated incidents, contract alerts.
- **`app/dags/etl_rh_dag.py`** — Airflow DAG. Each task is a `PythonOperator` wrapping the same ETL functions. Task order: `garantizar_tablas → etl_areas → etl_employees → [etl_vacaciones | etl_incidencias | etl_contract_alerts]`. All domain imports are deferred inside callables (not at parse time).
- **`app/utils/etl_core.py`** — `ejecutar_flujo_etl()` is the central orchestration function. It checks for cached staging data before calling the API, loads in 5,000-record batches, and cleans up staging on success.
- **`app/utils/db_client.py`** — Opens an SSH tunnel (`sshtunnel`) and returns a `psycopg2` connection. The tunnel is registered with `atexit` so it closes on process exit.
- **`app/utils/estado.py`** — Reads/writes `staging/estado_sincronizacion.json` to track daily task completion. A task marked `OK` for today is skipped on re-run.
- **`app/utils/staging_manager.py`** — Saves extracted data as JSON to `staging/` before loading begins. On a failed load, the cache persists and the next run retries the load without re-calling the API.
- **`app/etl/extract.py`** — All API extraction functions. Handles pagination automatically. Contains normalization helpers (`to_int_or_none`, `to_str_or_none`, date parsers).
- **`app/etl/load.py`** — Upsert functions per entity using `INSERT ... ON CONFLICT DO UPDATE`.
- **`app/etl/contract_alerts.py`** — Derives alert rows from employee/contract data already in the DB. No external API call — pure SQL logic applied in Python.
- **`app/etl/create.py`** — `garantizar_tablas_rh()` creates the `rh` schema and all tables idempotently on startup.
- **`app/config/settings.py`** — Loads all env vars from `.env`. Imported as `from app.config.settings import *`.

### Database Schema

All tables live in the `rh` schema:
- `rh.employees` — HR master data (~50 columns)
- `rh.areas` — Organizational structure
- `rh.vacations` — Vacation records
- `rh.consolidado_incidencias` — Unified incidents (licencias, ausencias, permisos)
- `rh.contract_alerts` — Contract expiration alerts (SEGUNDO_PLAZO / INDEFINIDO)

The DDL is in `creacion_tablas.sql` at the repo root and is also replicated programmatically in `app/etl/create.py`.

### Environment Configuration

All secrets live in `actualizacion-tablas-2/.env`. Required variables:

| Variable | Purpose |
|---|---|
| `TOKEN` | BUK API auth token |
| `API_BASE_URL` | BUK API base URL |
| `SSH_HOST`, `SSH_PORT`, `SSH_USER`, `SSH_PASSWORD` | SSH tunnel to DB server |
| `PG_HOST`, `PG_PORT`, `PG_DATABASE`, `PG_USER`, `PG_PASSWORD` | PostgreSQL credentials |
| `AIRFLOW_SECRET_KEY` | Required for Airflow mode (any random string) |

### Docker Files

| File | Purpose |
|---|---|
| `Dockerfile` | ETL standalone image (python:3.12-slim + MSSQL ODBC + app deps) |
| `Dockerfile.airflow` | Airflow image (apache/airflow:2.10.3-python3.12 + app deps only) |
| `compose.yml` | Standalone mode — single `rh_cramer_etl` container |
| `compose.airflow.yml` | Airflow mode — `airflow-db`, `airflow-init`, `airflow-scheduler`, `airflow-webserver` |

### Docker Volumes

The `compose.yml` mounts:
- `./logs` → `/app/logs` (daily log files: `Registro_inserciones_<date>.log`)
- `./staging` → `/app/staging` (JSON cache + `estado_sincronizacion.json` + error JSONL files)

The staging volume is critical — it persists task state and retry data across `docker compose down/up` cycles.

In Airflow mode (`compose.airflow.yml`), `./app` is also mounted read-only at `/opt/airflow/app_src/app` with `PYTHONPATH=/opt/airflow/app_src` so Airflow containers can import `app.*` modules.

### Fault Tolerance Pattern

1. Extract API data → save to `staging/<entity>_<date>.json`
2. Load data to DB in 5,000-record batches
3. On success → delete staging file, mark task `OK` in state JSON
4. On failure → staging file remains; next run skips extraction and retries load
5. Individual row errors are written to `staging/errores_etl_<date>.json` and can be retried via `reintentar_errores_entidad()` in `load.py`
