"""
End-to-End Historical Backfill Tests.
Tests partition interval selection, Silver/Gold rebuilding, and backfill run tracking.
"""

from __future__ import annotations

from src.orchestration.backfill_runner import BackfillCoordinator
from src.orchestration.pipeline_runner import LakehousePipelineRunner


def test_historical_backfill_execution(clean_metadata_tables):
    # Ensure baseline data is present by running initial pipeline
    init_runner = LakehousePipelineRunner(run_id="backfill_prep_run")
    init_runner.run_pipeline(
        batch_type="INITIAL",
        sample_order_count=30,
    )

    # Execute historical backfill
    res = BackfillCoordinator.execute_backfill(
        start_date="2026-01-01",
        end_date="2026-01-15",
        entities=["orders", "order_items", "customers", "products", "payments"],
    )

    assert res["status"] == "COMPLETED"
    assert res["backfill_id"].startswith("bf_")
    assert len(res["processed_partitions"]) == 5
    assert "mart_daily_sales" in res["gold_counts"]
