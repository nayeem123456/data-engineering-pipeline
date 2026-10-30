"""
Operational Metadata Management and Pipeline Observability.
Persists execution metrics, file manifests, task runs, and data quality results.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import text

from src.common.config import config
from src.common.db import db_manager
from src.common.logger import get_logger

logger = get_logger("platform.monitoring.operational_metadata")


class OperationalMetadata:
    """Manages recording and querying of operational metadata."""

    @classmethod
    def initialize_tables(cls) -> None:
        """Ensure operational metadata and serving tables are created."""
        engine = db_manager.get_engine()
        is_sqlite = db_manager.is_sqlite()

        if is_sqlite:
            # SQLite compatible DDL (no schemas, simple data types)
            statements = [
                """
                CREATE TABLE IF NOT EXISTS metadata_pipeline_runs (
                    run_id TEXT PRIMARY KEY,
                    pipeline_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP,
                    duration_seconds REAL,
                    error_message TEXT,
                    config_payload TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS metadata_task_runs (
                    task_run_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    task_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP,
                    duration_seconds REAL,
                    rows_in INTEGER DEFAULT 0,
                    rows_out INTEGER DEFAULT 0,
                    rows_rejected INTEGER DEFAULT 0,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS metadata_ingestion_batches (
                    batch_id TEXT PRIMARY KEY,
                    source_name TEXT NOT NULL,
                    entity TEXT NOT NULL,
                    batch_type TEXT NOT NULL,
                    record_count INTEGER DEFAULT 0,
                    file_count INTEGER DEFAULT 0,
                    ingestion_status TEXT NOT NULL,
                    started_at TIMESTAMP NOT NULL,
                    completed_at TIMESTAMP
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS metadata_source_file_manifests (
                    file_id TEXT PRIMARY KEY,
                    batch_id TEXT NOT NULL,
                    file_name TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    file_checksum_sha256 TEXT NOT NULL UNIQUE,
                    file_size_bytes INTEGER NOT NULL,
                    record_count INTEGER DEFAULT 0,
                    status TEXT NOT NULL,
                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS metadata_watermarks (
                    entity_name TEXT PRIMARY KEY,
                    watermark_type TEXT NOT NULL DEFAULT 'TIMESTAMP',
                    watermark_value TEXT NOT NULL,
                    last_batch_id TEXT NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS metadata_data_quality_results (
                    test_id TEXT PRIMARY KEY,
                    run_id TEXT,
                    layer TEXT NOT NULL,
                    entity TEXT NOT NULL,
                    rule_name TEXT NOT NULL,
                    rule_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    records_tested INTEGER DEFAULT 0,
                    records_failed INTEGER DEFAULT 0,
                    details TEXT,
                    tested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS metadata_quarantined_records (
                    quarantine_id TEXT PRIMARY KEY,
                    batch_id TEXT NOT NULL,
                    entity TEXT NOT NULL,
                    source_record_id TEXT,
                    raw_payload TEXT NOT NULL,
                    error_category TEXT NOT NULL,
                    error_message TEXT NOT NULL,
                    quarantined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS metadata_schema_change_events (
                    event_id TEXT PRIMARY KEY,
                    entity TEXT NOT NULL,
                    layer TEXT NOT NULL,
                    change_type TEXT NOT NULL,
                    previous_schema TEXT,
                    current_schema TEXT,
                    is_compatible INTEGER NOT NULL DEFAULT 1,
                    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS metadata_backfill_runs (
                    backfill_id TEXT PRIMARY KEY,
                    entity TEXT NOT NULL,
                    start_date TEXT NOT NULL,
                    end_date TEXT NOT NULL,
                    status TEXT NOT NULL,
                    processed_partitions TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS metadata_published_dataset_versions (
                    version_id TEXT PRIMARY KEY,
                    dataset_name TEXT NOT NULL,
                    layer TEXT NOT NULL,
                    version_tag TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    storage_path TEXT NOT NULL,
                    checksum TEXT NOT NULL,
                    published_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """,
            ]
            with engine.connect() as conn:
                for stmt in statements:
                    conn.execute(text(stmt))
                conn.commit()
            logger.info("Initialized operational metadata tables in SQLite.")
        else:
            # PostgreSQL execution
            sql_file = config.PROJECT_ROOT / "sql" / "01_init_metadata_schema.sql"
            if sql_file.exists():
                with open(sql_file, encoding="utf-8") as f:
                    ddl = f.read()
                with engine.connect() as conn:
                    conn.execute(text(ddl))
                    conn.commit()
                logger.info("Initialized operational metadata tables in PostgreSQL.")

    @classmethod
    def start_pipeline_run(
        cls, run_id: str, pipeline_name: str, config_payload: dict[str, Any] | None = None
    ) -> None:
        """Record the start of a pipeline run."""
        cls.initialize_tables()
        table = "metadata_pipeline_runs" if db_manager.is_sqlite() else "metadata.pipeline_runs"
        now = datetime.now(UTC)
        payload_str = json.dumps(config_payload or {})

        query = text(f"""
            INSERT INTO {table} (run_id, pipeline_name, status, start_time, config_payload)
            VALUES (:run_id, :name, 'RUNNING', :start_time, :payload)
        """)
        with db_manager.get_session() as session:
            session.execute(
                query,
                {
                    "run_id": run_id,
                    "name": pipeline_name,
                    "start_time": now,
                    "payload": payload_str,
                },
            )

    @classmethod
    def end_pipeline_run(cls, run_id: str, status: str, error_message: str | None = None) -> None:
        """Record pipeline run completion or failure."""
        table = "metadata_pipeline_runs" if db_manager.is_sqlite() else "metadata.pipeline_runs"
        now = datetime.now(UTC)

        # Calculate duration
        get_start_query = text(f"SELECT start_time FROM {table} WHERE run_id = :run_id")
        with db_manager.get_session() as session:
            res = session.execute(get_start_query, {"run_id": run_id}).fetchone()
            duration = None
            if res and res[0]:
                start_dt = res[0]
                if isinstance(start_dt, str):
                    start_dt = datetime.fromisoformat(start_dt)
                if start_dt.tzinfo is None:
                    start_dt = start_dt.replace(tzinfo=UTC)
                duration = round((now - start_dt).total_seconds(), 2)

            update_query = text(f"""
                UPDATE {table}
                SET status = :status, end_time = :end_time, duration_seconds = :duration, error_message = :err
                WHERE run_id = :run_id
            """)
            session.execute(
                update_query,
                {
                    "status": status,
                    "end_time": now,
                    "duration": duration,
                    "err": error_message,
                    "run_id": run_id,
                },
            )

    @classmethod
    def record_task_run(
        cls,
        run_id: str,
        task_name: str,
        status: str,
        start_time: datetime,
        end_time: datetime,
        rows_in: int = 0,
        rows_out: int = 0,
        rows_rejected: int = 0,
        error_message: str | None = None,
    ) -> None:
        """Record task-level execution telemetry."""
        table = "metadata_task_runs" if db_manager.is_sqlite() else "metadata.task_runs"
        task_run_id = f"task_{uuid.uuid4().hex[:12]}"
        duration = round((end_time - start_time).total_seconds(), 2)

        query = text(f"""
            INSERT INTO {table} (task_run_id, run_id, task_name, status, start_time, end_time, duration_seconds, rows_in, rows_out, rows_rejected, error_message)
            VALUES (:tid, :rid, :name, :status, :start, :end, :dur, :rin, :rout, :rrej, :err)
        """)
        with db_manager.get_session() as session:
            session.execute(
                query,
                {
                    "tid": task_run_id,
                    "rid": run_id,
                    "name": task_name,
                    "status": status,
                    "start": start_time,
                    "end": end_time,
                    "dur": duration,
                    "rin": rows_in,
                    "rout": rows_out,
                    "rrej": rows_rejected,
                    "err": error_message,
                },
            )

    @classmethod
    def record_quarantine_item(
        cls,
        batch_id: str,
        entity: str,
        source_record_id: str | None,
        raw_payload: str,
        error_category: str,
        error_message: str,
    ) -> None:
        """Store quarantined record metadata for audit and operator review."""
        table = (
            "metadata_quarantined_records"
            if db_manager.is_sqlite()
            else "metadata.quarantined_records"
        )
        qid = f"quarantine_{uuid.uuid4().hex[:12]}"
        now = datetime.now(UTC)

        query = text(f"""
            INSERT INTO {table} (quarantine_id, batch_id, entity, source_record_id, raw_payload, error_category, error_message, quarantined_at)
            VALUES (:qid, :bid, :entity, :srid, :payload, :cat, :msg, :now)
        """)
        with db_manager.get_session() as session:
            session.execute(
                query,
                {
                    "qid": qid,
                    "bid": batch_id,
                    "entity": entity,
                    "srid": source_record_id,
                    "payload": raw_payload,
                    "cat": error_category,
                    "msg": error_message,
                    "now": now,
                },
            )

    @classmethod
    def record_quality_result(
        cls,
        run_id: str | None,
        layer: str,
        entity: str,
        rule_name: str,
        rule_type: str,
        status: str,
        records_tested: int,
        records_failed: int,
        details: str | None = None,
    ) -> None:
        """Record individual quality check outcome."""
        table = (
            "metadata_data_quality_results"
            if db_manager.is_sqlite()
            else "metadata.data_quality_results"
        )
        test_id = f"dq_{uuid.uuid4().hex[:12]}"
        now = datetime.now(UTC)

        query = text(f"""
            INSERT INTO {table} (test_id, run_id, layer, entity, rule_name, rule_type, status, records_tested, records_failed, details, tested_at)
            VALUES (:tid, :rid, :layer, :entity, :rname, :rtype, :status, :tested, :failed, :details, :now)
        """)
        with db_manager.get_session() as session:
            session.execute(
                query,
                {
                    "tid": test_id,
                    "rid": run_id,
                    "layer": layer,
                    "entity": entity,
                    "rname": rule_name,
                    "rtype": rule_type,
                    "status": status,
                    "tested": records_tested,
                    "failed": records_failed,
                    "details": details,
                    "now": now,
                },
            )

    @classmethod
    def record_published_version(
        cls,
        dataset_name: str,
        layer: str,
        version_tag: str,
        row_count: int,
        storage_path: str,
        checksum: str,
    ) -> None:
        """Record successfully published dataset version."""
        table = (
            "metadata_published_dataset_versions"
            if db_manager.is_sqlite()
            else "metadata.published_dataset_versions"
        )
        vid = f"ver_{uuid.uuid4().hex[:12]}"
        now = datetime.now(UTC)

        query = text(f"""
            INSERT INTO {table} (version_id, dataset_name, layer, version_tag, row_count, storage_path, checksum, published_at)
            VALUES (:vid, :name, :layer, :tag, :count, :path, :chk, :now)
        """)
        with db_manager.get_session() as session:
            session.execute(
                query,
                {
                    "vid": vid,
                    "name": dataset_name,
                    "layer": layer,
                    "tag": version_tag,
                    "count": row_count,
                    "path": storage_path,
                    "chk": checksum,
                    "now": now,
                },
            )
