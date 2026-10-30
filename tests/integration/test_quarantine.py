"""
Integration Tests for Quarantine Isolation.
"""

from __future__ import annotations

import json

from src.common.config import config
from src.storage.lakehouse_fs import LakehouseFS
from src.validation.quarantine import QuarantineManager


def test_quarantine_isolation_and_persistence(clean_metadata_tables):
    bad_records = [
        {
            "order_id": "BAD-ORD-901",
            "customer_id": "CUST-01",
            "total_amount": -50.0,
            "_quarantine_reason": "Negative total amount detected (-50.0)",
        },
        {
            "order_id": "BAD-ORD-902",
            "customer_id": "CUST-02",
            "total_amount": -10.0,
            "_quarantine_reason": "Negative total amount detected (-10.0)",
        },
    ]

    count = QuarantineManager.quarantine_records(
        invalid_rows=bad_records,
        entity="orders",
        batch_id="test_quarantine_batch",
        error_category="DATA_VALIDATION_ERROR",
        default_error_message="Validation error",
        id_field="order_id",
    )

    assert count == 2

    # Check parquet file in quarantine directory
    quar_dir = config.QUARANTINE_DIR / "orders"
    assert quar_dir.exists()
    parquet_files = list(quar_dir.glob("**/*.parquet"))
    assert len(parquet_files) > 0

    df_quar = LakehouseFS.read_parquet(parquet_files[0])
    assert len(df_quar) == 2
    assert "error_category" in df_quar.columns
    assert "raw_payload" in df_quar.columns
    assert df_quar.iloc[0]["source_record_id"] == "BAD-ORD-901"

    # Verify payload is valid JSON and preserves original data
    payload = json.loads(df_quar.iloc[0]["raw_payload"])
    assert payload["order_id"] == "BAD-ORD-901"
    assert payload["total_amount"] == -50.0
