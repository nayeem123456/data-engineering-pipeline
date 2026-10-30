"""
Integration Tests for Data Quality Validation Engine and Gatekeeper.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.common.config import config
from src.storage.lakehouse_fs import LakehouseFS
from src.validation.quality_engine import DataQualityGateError, QualityEngine


def test_bronze_quality_gate_detects_corrupt_or_missing_columns():
    # Valid Bronze file
    valid_df = pd.DataFrame(
        [
            {
                "_ingestion_id": "ing_1",
                "_batch_id": "b_1",
                "_source_file_checksum": "chk_123",
                "_ingested_at": "2026-01-01T00:00:00Z",
                "data_col": "value",
            }
        ]
    )
    valid_path = config.BRONZE_DIR / "orders" / "valid_test.parquet"
    LakehouseFS.write_parquet(valid_df, valid_path)

    res = QualityEngine.validate_bronze_layer("orders", valid_path)
    assert res["status"] == "PASS"

    # Invalid Bronze file missing required audit column
    bad_df = pd.DataFrame([{"data_col": "value"}])
    bad_path = config.BRONZE_DIR / "orders" / "bad_test.parquet"
    LakehouseFS.write_parquet(bad_df, bad_path)

    with pytest.raises(DataQualityGateError):
        QualityEngine.validate_bronze_layer("orders", bad_path)


def test_silver_quality_gate_passes_on_valid_data():
    # Write valid Silver orders
    orders_df = pd.DataFrame(
        [
            {
                "order_id": "ORD-VALID-01",
                "customer_id": "CUST-VALID-01",
                "order_status": "COMPLETED",
                "total_amount": 120.0,
                "subtotal_amount": 100.0,
            }
        ]
    )
    LakehouseFS.write_silver_dataset(orders_df, "orders")

    results = QualityEngine.validate_silver_layer(entity_filter="orders")
    assert len(results) > 0
    assert all(r["status"] == "PASS" for r in results)
