# Operational Runbook: E-Commerce Lakehouse & ETL Platform

This runbook provides actionable procedures for data platform engineers and operations teams to deploy, operate, monitor, troubleshoot, and backfill the Data Lakehouse platform.

---

## 1. System Setup & Architecture Modes

The platform supports two deployment profiles:
1. **Containerized Production Profile:** Docker Compose running PostgreSQL 15, Apache Airflow (Webserver + Scheduler), Spark Lakehouse Worker, and Streamlit Dashboard.
2. **Local Developer Profile:** Lightweight execution using Python 3.11, DuckDB/PyArrow engine, and local SQLite metadata fallback (`data/metadata_store.db`).

### Starting the Platform (Docker)
```bash
# 1. Copy sample environment variables
cp .env.example .env

# 2. Build and launch all container services in detached mode
docker compose up -d --build

# 3. Check container health status
docker compose ps
```

Service endpoints:
- **Airflow Webserver:** [http://localhost:8080](http://localhost:8080) (Default user: `airflow` / `airflow`)
- **Streamlit Observability Dashboard:** [http://localhost:8501](http://localhost:8501)
- **PostgreSQL Database:** `localhost:5432` (User: `lakehouse_admin`, DB: `lakehouse_db`)

### Starting the Platform (Local Developer Mode)
```bash
# 1. Activate Python virtual environment
source .venv/bin/activate  # Or .\.venv\Scripts\Activate.ps1 on Windows

# 2. Run automated test suite to verify environment integrity
pytest tests/ -v

# 3. Launch the Streamlit Observability Dashboard
streamlit run dashboard/app.py
```

---

## 2. Daily Pipeline Execution

### Scheduled Automated Runs
The Airflow DAG `ecommerce_lakehouse_pipeline` is scheduled to run daily at `02:00 UTC` (`@daily`).
- **Watermark tracking:** Automatically queries `metadata.watermarks` to fetch incremental records since the last successful execution.
- **Fail-safe publication:** The Gold dimensional models and marts are published **only** if all Critical Data Quality gates pass.

### Manual Pipeline Execution via CLI
Operators can trigger an immediate full-tier pipeline run via the command line:

```bash
# Ingest raw data, clean to Silver, build Gold marts, and publish
python scripts/run_pipeline.py --ingest --silver --gold --publish

# To generate fresh realistic mock source data prior to ingestion:
python scripts/generate_mock_data.py --orders 1500 --customers 300 --products 80
python scripts/run_pipeline.py --all
```

---

## 3. Historical Backfill Procedure

Backfills recompute downstream partitions without causing duplicate keys, inflating revenue, or corrupting published historical aggregates.

### Execution via CLI
To reprocess historical partitions for a specific date window:
```bash
# Backfill a historical date range
python scripts/run_backfill.py --start-date 2026-01-01 --end-date 2026-03-31 --entity orders

# Optional dry-run mode to inspect affected partitions without committing:
python scripts/run_backfill.py --start-date 2026-01-01 --end-date 2026-01-31 --dry-run
```

### Execution via Apache Airflow
1. Navigate to the Airflow UI at `http://localhost:8080`.
2. Locate the DAG `ecommerce_backfill_dag`.
3. Click **Trigger DAG w/ config** and submit the following JSON configuration:
```json
{
  "start_date": "2026-01-01",
  "end_date": "2026-03-31",
  "entities": ["orders", "order_items", "payments"],
  "recompute_gold": true
}
```
4. Monitor the task progress. The DAG will:
   - Identify affected partitions in `data/lakehouse/bronze/`.
   - Reprocess and overwrite specific partitions in `data/lakehouse/silver/`.
   - Atomically rebuild affected date records in `fact_orders` and `mart_daily_sales`.
   - Log the execution record to `metadata.backfill_runs`.

---

## 4. Operational Incident Handling

### Incident 1: File Ingestion Blocked (Duplicate File Detected)
- **Symptom:** Pipeline reports `FILE_ALREADY_INGESTED` and skips processing.
- **Root Cause:** SHA-256 fingerprint matching in `metadata.file_manifests` indicates the file was already processed.
- **Remediation:**
  1. Inspect `metadata.file_manifests` for the file's hash and previous batch ID.
  2. If the file is a re-sent batch with legitimate corrected records, execute an override backfill or rename with a timestamp suffix.

### Incident 2: Malformed Records / Missing Columns (Schema Evolution)
- **Symptom:** Warnings in logs; row counts in Silver lower than Bronze.
- **Root Cause:** New or corrupt source files containing non-conforming types or missing required business keys.
- **Remediation:**
  1. Open the **Streamlit Dashboard** and navigate to **Quarantine & Drift**.
  2. Inspect the failed records and rejection reasons (e.g. `MISSING_REQUIRED_KEY: order_id`, `INVALID_RANGE: total_amount < 0`).
  3. Quarantined records are isolated at `data/lakehouse/quarantine/` and recorded in `metadata.quarantined_records`.
  4. Fix upstream source data generation or update schema definition in `schemas/silver_schemas.json` if schema drift is legitimate.

### Incident 3: Data Quality Gate Failure (Pipeline Halted)
- **Symptom:** Airflow task `validate_silver_quality` or `validate_gold_quality` fails with `CRITICAL_QUALITY_FAILURE`.
- **Root Cause:** Automated assertion failed (e.g. negative revenue detected, duplicate primary keys found, or revenue reconciliation mismatch).
- **Remediation:**
  1. Query `metadata.data_quality_results` where `passed = FALSE`:
     ```sql
     SELECT check_name, dataset_name, severity, error_message, metric_value
     FROM metadata.data_quality_results
     WHERE passed = FALSE
     ORDER BY execution_timestamp DESC LIMIT 10;
     ```
  2. Gold models are **not** published when a CRITICAL check fails, protecting downstream consumers.
  3. Address the source issue or update quality rule thresholds in `src/validation/rules.py`.

### Incident 4: Database Connection Failure (PostgreSQL Outage)
- **Symptom:** Ingestion or pipeline logs report `DATABASE_CONNECTION_ERROR`.
- **Root Cause:** PostgreSQL container unhealthy or network partition.
- **Remediation:**
  1. Verify container status: `docker compose ps postgres`.
  2. Check PostgreSQL logs: `docker compose logs postgres --tail 50`.
  3. In local environments, the platform's automatic SQLite fallback activates if PostgreSQL is unavailable.

---

## 5. Maintenance & Housekeeping

### Data Lake Reset & Clean Start
To clear all local runtime data and start fresh:
```bash
# Using Makefile
make clean

# Manual cleanup:
rm -rf data/lakehouse/bronze/*
rm -rf data/lakehouse/silver/*
rm -rf data/lakehouse/gold/*
rm -rf data/lakehouse/quarantine/*
rm -f data/metadata_store.db
```

### Partition Compaction & Cleanup
To consolidate small Parquet files produced by high-frequency micro-batches:
```bash
python scripts/run_pipeline.py --compact --layer silver
```
