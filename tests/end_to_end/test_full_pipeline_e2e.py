"""
End-to-End Pipeline Integration Test.
Executes the full pipeline flow from synthetic data generation through Bronze, Silver, Gold,
reconciliation gates, and serving table sync.
"""

from __future__ import annotations

from src.common.config import config
from src.orchestration.pipeline_runner import LakehousePipelineRunner
from src.storage.lakehouse_fs import LakehouseFS


def test_full_pipeline_end_to_end(clean_metadata_tables):
    runner = LakehousePipelineRunner(run_id="e2e_full_test")

    metrics = runner.run_pipeline(
        batch_type="INITIAL",
        sample_order_count=60,
        inject_invalid=True,  # Test quarantine isolation simultaneously
        inject_duplicates=True,  # Test deduplication simultaneously
    )

    assert metrics["status"] == "SUCCESS"
    assert metrics["bronze_rows"] > 0
    assert metrics["silver_rows"] > 0
    assert metrics["quarantined_rows"] > 0  # Injected invalid records properly quarantined!

    # Verify Bronze Parquet files exist
    assert (config.BRONZE_DIR / "orders").exists()
    assert (config.BRONZE_DIR / "customers").exists()

    # Verify Silver Parquet files exist
    assert (config.SILVER_DIR / "orders" / "data.parquet").exists()
    assert (config.SILVER_DIR / "customers" / "data.parquet").exists()

    # Verify Gold Parquet tables exist
    assert (config.GOLD_DIR / "dim_customers" / "data.parquet").exists()
    assert (config.GOLD_DIR / "dim_products" / "data.parquet").exists()
    assert (config.GOLD_DIR / "dim_date" / "data.parquet").exists()
    assert (config.GOLD_DIR / "fact_orders" / "data.parquet").exists()
    assert (config.GOLD_DIR / "fact_order_items" / "data.parquet").exists()
    assert (config.GOLD_DIR / "fact_payments" / "data.parquet").exists()
    assert (config.GOLD_DIR / "mart_daily_sales" / "data.parquet").exists()
    assert (config.GOLD_DIR / "mart_customer_lifetime_value" / "data.parquet").exists()
    assert (config.GOLD_DIR / "mart_product_performance" / "data.parquet").exists()
    assert (config.GOLD_DIR / "mart_refund_analysis" / "data.parquet").exists()

    # Verify Financial Reconciliation between fact_orders and mart_daily_sales
    fact_orders_df = LakehouseFS.read_parquet(config.GOLD_DIR / "fact_orders" / "data.parquet")
    daily_sales_df = LakehouseFS.read_parquet(config.GOLD_DIR / "mart_daily_sales" / "data.parquet")

    fact_revenue = fact_orders_df["total_amount"].sum()
    mart_revenue = daily_sales_df["gross_revenue"].sum()

    assert abs(fact_revenue - mart_revenue) < 0.05, (
        f"Reconciliation discrepancy: fact={fact_revenue}, mart={mart_revenue}"
    )
