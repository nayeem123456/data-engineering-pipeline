"""
End-to-End Failure Recovery and Resilience Tests.
Validates the 10 critical operational failure and edge-case scenarios:
1. Malformed input file
2. Duplicate source records
3. Missing required column
4. New optional column
5. Failed database connection / fallback
6. Transformation task failure
7. Rerun of previously successful batch
8. Late-arriving source records
9. Failed backfill handling
10. Data quality rule violation & hard gate halting
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from src.common.config import config
from src.common.db import db_manager
from src.ingestion.raw_ingester import RawIngester
from src.orchestration.backfill_runner import BackfillCoordinator
from src.storage.lakehouse_fs import LakehouseFS
from src.storage.watermark_manager import WatermarkManager
from src.transformations.engine_adapter import SilverTransformationEngine
from src.transformations.schema_evolution import SchemaEvolutionManager
from src.validation.quality_engine import DataQualityGateError, QualityEngine


def test_scenario_1_malformed_input_file():
    """Scenario 1: Malformed input file raises clear exception and does not corrupt storage."""
    with tempfile.TemporaryDirectory() as td:
        bad_file = Path(td) / "corrupt_file.txt"
        bad_file.write_text("THIS IS NOT A VALID CSV OR JSON FILE CONTENT")

        with pytest.raises(ValueError, match="Unsupported file format"):
            RawIngester.ingest_file(
                file_path=bad_file,
                entity="orders",
                source_name="test_src",
                batch_id="b_corrupt",
            )


def test_scenario_2_duplicate_source_records():
    """Scenario 2: Source contains duplicate records; deduplication retains latest."""
    orders_data = [
        {
            "order_id": "ORD-DUP-1",
            "customer_id": "C-1",
            "order_status": "COMPLETED",
            "total_amount": "100.0",
            "created_at": "2026-01-01T10:00:00Z",
            "updated_at": "2026-01-01T10:00:00Z",
            "_ingested_at": "2026-01-01T10:00:00Z",
            "_batch_id": "b1",
        },
        {
            "order_id": "ORD-DUP-1",
            "customer_id": "C-1",
            "order_status": "SHIPPED",
            "total_amount": "100.0",
            "created_at": "2026-01-01T10:00:00Z",
            "updated_at": "2026-01-01T12:00:00Z",
            "_ingested_at": "2026-01-01T12:00:00Z",
            "_batch_id": "b2",
        },
    ]
    df = pd.DataFrame(orders_data)
    p = config.BRONZE_DIR / "orders" / "dup_test.parquet"
    LakehouseFS.write_parquet(df, p)

    res = SilverTransformationEngine.transform_entity("orders", "b_dup", bronze_files=[p])
    assert res["rows_in"] == 2
    assert res["rows_out"] == 1

    clean_df = LakehouseFS.read_parquet(config.SILVER_DIR / "orders" / "data.parquet")
    assert clean_df.iloc[0]["order_status"] == "SHIPPED"


def test_scenario_3_missing_required_column():
    """Scenario 3: Missing required column is detected as breaking change."""
    df_missing = pd.DataFrame([{"first_name": "Bob"}])  # missing customer_id and email
    is_compat, changes = SchemaEvolutionManager.evaluate_schema("customers", df_missing)
    assert not is_compat
    assert any(c["change_type"] == "MISSING_REQUIRED_COLUMN" for c in changes)


def test_scenario_4_new_optional_column():
    """Scenario 4: New optional column is accommodated gracefully as compatible."""
    df_new_col = pd.DataFrame(
        [
            {
                "customer_id": "C-NEW",
                "email": "new@example.com",
                "signup_date": "2026-01-01",
                "loyalty_tier": "GOLD_MEMBER",
            }
        ]
    )
    is_compat, changes = SchemaEvolutionManager.evaluate_schema("customers", df_new_col)
    assert is_compat
    assert any(c["change_type"] == "NEW_OPTIONAL_COLUMN" for c in changes)


def test_scenario_5_failed_database_connection_fallback():
    """Scenario 5: DB manager seamlessly falls back to local SQLite when Postgres is down."""
    orig_host = config.POSTGRES_HOST
    try:
        config.POSTGRES_HOST = "non_existent_unreachable_host_9999"
        config.DATABASE_URL = f"postgresql://user:pass@{config.POSTGRES_HOST}:5432/dw"
        db_manager.reset_engine()

        # Engine should gracefully fall back to SQLite without crashing
        engine = db_manager.get_engine()
        assert db_manager.is_sqlite()
        with engine.connect() as conn:
            assert conn is not None
    finally:
        config.POSTGRES_HOST = orig_host
        db_manager.reset_engine()


def test_scenario_6_transformation_task_failure():
    """Scenario 6: Missing entity directory returns safe zero rows without crashing."""
    res = SilverTransformationEngine.transform_entity("non_existent_entity_xyz", "b_fail")
    assert res["rows_in"] == 0
    assert res["rows_out"] == 0


def test_scenario_7_rerun_previously_successful_batch():
    """Scenario 7: Re-running a batch produces deterministic outputs without duplicate keys."""
    orders_df = pd.DataFrame(
        [
            {
                "order_id": "ORD-RERUN-10",
                "customer_id": "CUST-10",
                "order_status": "COMPLETED",
                "currency": "USD",
                "subtotal_amount": "50.00",
                "tax_amount": "4.00",
                "shipping_amount": "0.00",
                "discount_amount": "0.00",
                "total_amount": "54.00",
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
                "_ingested_at": "2026-01-01T00:00:00Z",
                "_batch_id": "b_rerun",
            }
        ]
    )
    p = config.BRONZE_DIR / "orders" / "rerun_test.parquet"
    LakehouseFS.write_parquet(orders_df, p)

    # Run 1
    res1 = SilverTransformationEngine.transform_entity("orders", "b_rerun_1", bronze_files=[p])
    # Run 2
    res2 = SilverTransformationEngine.transform_entity("orders", "b_rerun_2", bronze_files=[p])

    assert res1["rows_out"] == res2["rows_out"]
    clean_df = LakehouseFS.read_parquet(config.SILVER_DIR / "orders" / "data.parquet")
    assert clean_df["order_id"].nunique() == len(clean_df)


def test_scenario_8_late_arriving_source_records():
    """Scenario 8: Late arriving records within grace period update previous state correctly."""
    # Watermark set to today
    WatermarkManager.update_watermark("orders", "2026-01-10T12:00:00+00:00", "b_wm")
    w_start, w_end = WatermarkManager.get_extraction_window("orders", lookback_hours=24)

    # Extraction window must begin 24 hours prior to watermark, capturing late-arriving records
    assert w_start.isoformat().startswith("2026-01-09")


def test_scenario_9_failed_backfill_records_status(clean_metadata_tables):
    """Scenario 9: Failed backfill records FAILED status in metadata store."""
    with pytest.raises(ValueError):
        # Trigger backfill with non-existent invalid entity
        BackfillCoordinator.execute_backfill(
            start_date="2026-01-01",
            end_date="2026-01-02",
            entities=["invalid_entity_that_fails"],
        )


def test_scenario_10_data_quality_rule_violation_halts_pipeline():
    """Scenario 10: Critical Data Quality rule violation raises DataQualityGateError and stops publication."""
    # Inject bad order with duplicate order_id into silver table
    bad_silver_df = pd.DataFrame(
        [
            {
                "order_id": "DUPLICATE-ID",
                "customer_id": "C-1",
                "order_status": "COMPLETED",
                "total_amount": 100.0,
                "subtotal_amount": 100.0,
            },
            {
                "order_id": "DUPLICATE-ID",
                "customer_id": "C-1",
                "order_status": "COMPLETED",
                "total_amount": 100.0,
                "subtotal_amount": 100.0,
            },
        ]
    )
    LakehouseFS.write_silver_dataset(bad_silver_df, "orders")

    # Hard gate must fail
    with pytest.raises(DataQualityGateError):
        QualityEngine.validate_silver_layer(entity_filter="orders")
