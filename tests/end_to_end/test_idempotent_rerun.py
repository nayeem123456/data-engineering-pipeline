"""
End-to-End Idempotency and Pipeline Replay Tests.
Proves that re-executing the same batch multiple times produces deterministic outputs,
does not duplicate records, and does not inflate analytical metrics.
"""

from __future__ import annotations

from src.common.config import config
from src.ingestion.generator import ECommerceDataGenerator
from src.orchestration.pipeline_runner import LakehousePipelineRunner
from src.storage.lakehouse_fs import LakehouseFS


def test_pipeline_idempotency_on_repeated_runs(clean_metadata_tables):
    # 1. Generate a fixed sample dataset
    gen = ECommerceDataGenerator()
    products = gen.generate_products(count=15)
    customers = gen.generate_customers(count=20)
    batch_data = gen.generate_orders_and_related(
        customers=customers,
        products=products,
        order_count=40,
        inject_invalid=False,
        inject_duplicates=False,
    )
    batch_data["products"] = products
    batch_data["customers"] = customers

    # Save to fixed staging path
    staging_dir = config.DATA_DIR / "staging" / "fixed_idempotent_test"
    files = gen.write_dataset_to_staging(
        dataset=batch_data,
        staging_dir=staging_dir,
        batch_id="fixed_batch_idempotent",
        format_type="json",
    )

    runner = LakehousePipelineRunner(run_id="idempotent_run_1")

    # Run 1: Initial execution
    metrics_1 = runner.run_pipeline(
        batch_type="INITIAL",
        generate_sample_data=False,
        staging_files=files,
    )
    assert metrics_1["status"] == "SUCCESS"

    silver_orders_1 = LakehouseFS.read_parquet(config.SILVER_DIR / "orders" / "data.parquet")
    gold_orders_1 = LakehouseFS.read_parquet(config.GOLD_DIR / "fact_orders" / "data.parquet")
    daily_sales_1 = LakehouseFS.read_parquet(config.GOLD_DIR / "mart_daily_sales" / "data.parquet")

    initial_orders_count = len(silver_orders_1)
    initial_revenue = daily_sales_1["gross_revenue"].sum()

    # Run 2: Exact same pipeline execution (replay / rerun)
    runner_2 = LakehousePipelineRunner(run_id="idempotent_run_2")
    metrics_2 = runner_2.run_pipeline(
        batch_type="INCREMENTAL",
        generate_sample_data=False,
        staging_files=files,
    )
    assert metrics_2["status"] == "SUCCESS"

    silver_orders_2 = LakehouseFS.read_parquet(config.SILVER_DIR / "orders" / "data.parquet")
    gold_orders_2 = LakehouseFS.read_parquet(config.GOLD_DIR / "fact_orders" / "data.parquet")
    daily_sales_2 = LakehouseFS.read_parquet(config.GOLD_DIR / "mart_daily_sales" / "data.parquet")

    rerun_orders_count = len(silver_orders_2)
    rerun_revenue = daily_sales_2["gross_revenue"].sum()

    # CRITICAL IDEMPOTENCY ASSERTIONS (Run 2 vs Run 1):
    # 1. Row count must remain identical
    assert rerun_orders_count == initial_orders_count, "Order row count changed on repeated run!"
    # 2. Revenue must not be inflated or doubled
    assert round(rerun_revenue, 2) == round(initial_revenue, 2), "Revenue inflated on repeated run!"
    # 3. Fact orders count must remain identical
    assert len(gold_orders_2) == len(gold_orders_1)
    # 4. Zero duplicate order_id business keys
    assert silver_orders_2["order_id"].nunique() == len(silver_orders_2)

    # Run 3: Exact same execution for a THIRD time
    runner_3 = LakehousePipelineRunner(run_id="idempotent_run_3")
    metrics_3 = runner_3.run_pipeline(
        batch_type="INCREMENTAL",
        generate_sample_data=False,
        staging_files=files,
    )
    assert metrics_3["status"] == "SUCCESS"

    silver_orders_3 = LakehouseFS.read_parquet(config.SILVER_DIR / "orders" / "data.parquet")
    daily_sales_3 = LakehouseFS.read_parquet(config.GOLD_DIR / "mart_daily_sales" / "data.parquet")
    third_run_revenue = daily_sales_3["gross_revenue"].sum()

    assert len(silver_orders_3) == initial_orders_count, "Order count changed on third execution!"
    assert round(third_run_revenue, 2) == round(initial_revenue, 2), (
        "Revenue inflated on third execution!"
    )
    assert silver_orders_3["order_id"].nunique() == len(silver_orders_3)


def test_idempotency_with_late_arriving_status_update(clean_metadata_tables):
    """Late-arriving update to existing order updates state without duplicating row count."""
    import pandas as pd

    from src.transformations.engine_adapter import SilverTransformationEngine

    # Initial order batch
    order_v1 = pd.DataFrame(
        [
            {
                "order_id": "ORD-LATE-101",
                "customer_id": "CUST-101",
                "order_status": "PROCESSING",
                "currency": "USD",
                "subtotal_amount": "80.00",
                "tax_amount": "5.00",
                "shipping_amount": "0.00",
                "discount_amount": "0.00",
                "total_amount": "85.00",
                "created_at": "2026-01-01T10:00:00Z",
                "updated_at": "2026-01-01T10:00:00Z",
                "_ingested_at": "2026-01-01T10:00:00Z",
                "_batch_id": "b_late_1",
            }
        ]
    )
    p1 = config.BRONZE_DIR / "orders" / "late_v1.parquet"
    LakehouseFS.write_parquet(order_v1, p1)
    res1 = SilverTransformationEngine.transform_entity("orders", "b_late_1", bronze_files=[p1])
    assert res1["rows_out"] == 1

    silver_v1 = LakehouseFS.read_parquet(config.SILVER_DIR / "orders" / "data.parquet")
    assert (
        silver_v1.loc[silver_v1["order_id"] == "ORD-LATE-101", "order_status"].iloc[0]
        == "PROCESSING"
    )

    # Late arriving update: same order, updated status to DELIVERED with later timestamp
    order_v2 = pd.DataFrame(
        [
            {
                "order_id": "ORD-LATE-101",
                "customer_id": "CUST-101",
                "order_status": "COMPLETED",
                "currency": "USD",
                "subtotal_amount": "80.00",
                "tax_amount": "5.00",
                "shipping_amount": "0.00",
                "discount_amount": "0.00",
                "total_amount": "85.00",
                "created_at": "2026-01-01T10:00:00Z",
                "updated_at": "2026-01-02T15:00:00Z",
                "_ingested_at": "2026-01-02T15:00:00Z",
                "_batch_id": "b_late_2",
            }
        ]
    )
    p2 = config.BRONZE_DIR / "orders" / "late_v2.parquet"
    LakehouseFS.write_parquet(order_v2, p2)
    res2 = SilverTransformationEngine.transform_entity("orders", "b_late_2", bronze_files=[p2])
    assert res2["rows_out"] == 1

    silver_v2 = LakehouseFS.read_parquet(config.SILVER_DIR / "orders" / "data.parquet")
    matching = silver_v2[silver_v2["order_id"] == "ORD-LATE-101"]
    assert len(matching) == 1, "Duplicate order_id created on late arriving update!"
    assert matching["order_status"].iloc[0] == "COMPLETED", (
        "Order status failed to update to latest state!"
    )
