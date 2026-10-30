"""
Unit Tests for Deterministic Business Key Deduplication.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd

from src.common.config import config
from src.storage.lakehouse_fs import LakehouseFS
from src.transformations.engine_adapter import SilverTransformationEngine


def test_deterministic_deduplication_preserves_latest_record():
    with tempfile.TemporaryDirectory() as td:
        Path(td)
        bronze_order_dir = config.BRONZE_DIR / "orders" / "ingestion_date=2026-01-15"
        bronze_order_dir.mkdir(parents=True, exist_ok=True)

        # Create two versions of the same order: first COMPLETED, second updated to REFUNDED later
        order_v1 = {
            "order_id": "ORD-DEDUP-99",
            "customer_id": "CUST-01",
            "order_status": "COMPLETED",
            "currency": "USD",
            "subtotal_amount": "100.00",
            "tax_amount": "8.00",
            "shipping_amount": "0.00",
            "discount_amount": "0.00",
            "total_amount": "108.00",
            "created_at": "2026-01-15T10:00:00Z",
            "updated_at": "2026-01-15T10:00:00Z",
            "_ingested_at": "2026-01-15T10:05:00Z",
            "_batch_id": "b1",
        }
        order_v2 = {
            "order_id": "ORD-DEDUP-99",
            "customer_id": "CUST-01",
            "order_status": "REFUNDED",
            "currency": "USD",
            "subtotal_amount": "100.00",
            "tax_amount": "8.00",
            "shipping_amount": "0.00",
            "discount_amount": "0.00",
            "total_amount": "108.00",
            "created_at": "2026-01-15T10:00:00Z",
            "updated_at": "2026-01-15T14:30:00Z",  # newer update
            "_ingested_at": "2026-01-15T14:35:00Z",
            "_batch_id": "b2",
        }

        df = pd.DataFrame([order_v1, order_v2])
        parquet_file = bronze_order_dir / "batch_dedup_test.parquet"
        LakehouseFS.write_parquet(df, parquet_file)

        # Transform using SilverTransformationEngine
        res = SilverTransformationEngine.transform_entity(
            entity="orders",
            batch_id="test_dedup_run",
            bronze_files=[parquet_file],
        )

        assert res["rows_in"] == 2
        assert res["rows_out"] == 1  # exactly 1 deduped row!

        silver_file = config.SILVER_DIR / "orders" / "data.parquet"
        clean_df = LakehouseFS.read_parquet(silver_file)
        assert len(clean_df) == 1
        # The latest state should be REFUNDED
        assert clean_df.iloc[0]["order_status"] == "REFUNDED"
