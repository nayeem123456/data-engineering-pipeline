"""
Pytest Fixtures and Isolated Test Environment Configuration.
Provides temporary isolated directories, fresh test databases, and reproducible datasets.
"""

from __future__ import annotations

import shutil
import sys
import tempfile
from collections.abc import Generator
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import contextlib

import pytest
from sqlalchemy import text

from src.common.config import config
from src.common.db import db_manager
from src.monitoring.operational_metadata import OperationalMetadata


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment() -> Generator[None, None, None]:
    """Configure isolated temporary test directories for the entire test session."""
    temp_dir = tempfile.mkdtemp(prefix="lakehouse_test_")
    test_path = Path(temp_dir)

    # Override config paths for clean isolation
    orig_data = config.DATA_DIR
    orig_bronze = config.BRONZE_DIR
    orig_silver = config.SILVER_DIR
    orig_gold = config.GOLD_DIR
    orig_quarantine = config.QUARANTINE_DIR
    orig_db = config.SQLITE_DB_PATH

    config.DATA_DIR = test_path / "data"
    config.LAKEHOUSE_BASE = test_path / "data" / "lakehouse"
    config.BRONZE_DIR = test_path / "data" / "lakehouse" / "bronze"
    config.SILVER_DIR = test_path / "data" / "lakehouse" / "silver"
    config.GOLD_DIR = test_path / "data" / "lakehouse" / "gold"
    config.QUARANTINE_DIR = test_path / "data" / "lakehouse" / "quarantine"
    config.SQLITE_DB_PATH = test_path / "test_metadata.db"
    config.USE_SQLITE_METADATA_FALLBACK = True

    config.ensure_directories()
    db_manager.reset_engine()
    OperationalMetadata.initialize_tables()

    yield

    # Teardown
    db_manager.reset_engine()
    shutil.rmtree(temp_dir, ignore_errors=True)

    # Restore
    config.DATA_DIR = orig_data
    config.BRONZE_DIR = orig_bronze
    config.SILVER_DIR = orig_silver
    config.GOLD_DIR = orig_gold
    config.QUARANTINE_DIR = orig_quarantine
    config.SQLITE_DB_PATH = orig_db


@pytest.fixture(autouse=True)
def clean_metadata_tables() -> Generator[None, None, None]:
    """Ensure metadata tables and lakehouse storage partitions are freshly cleaned for each test."""
    engine = db_manager.get_engine()
    with engine.connect() as conn:
        for tbl in [
            "metadata_pipeline_runs",
            "metadata_task_runs",
            "metadata_ingestion_batches",
            "metadata_source_file_manifests",
            "metadata_watermarks",
            "metadata_data_quality_results",
            "metadata_quarantined_records",
            "metadata_schema_change_events",
            "metadata_backfill_runs",
            "metadata_published_dataset_versions",
            "dim_date",
            "dim_customers",
            "dim_products",
            "fact_orders",
            "fact_order_items",
            "fact_payments",
            "mart_daily_sales",
            "mart_customer_lifetime_value",
            "mart_product_performance",
            "mart_refund_analysis",
        ]:
            with contextlib.suppress(Exception):
                conn.execute(text(f"DROP TABLE IF EXISTS {tbl};"))
        conn.commit()

    for dir_path in [
        config.BRONZE_DIR,
        config.SILVER_DIR,
        config.GOLD_DIR,
        config.QUARANTINE_DIR,
        config.DATA_DIR / "staging",
    ]:
        if dir_path.exists():
            shutil.rmtree(dir_path, ignore_errors=True)

    config.ensure_directories()
    OperationalMetadata.initialize_tables()
    yield
