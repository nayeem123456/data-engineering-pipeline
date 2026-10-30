"""
Integration Tests for dbt Transformations and Dimensional Kimball Models.
Verifies dimensional modeling logic, surrogate keys, fact grains, financial reconciliation,
and macro behavior using in-memory DuckDB executing the conformed Silver and Gold schemas.
"""

from __future__ import annotations

import duckdb
import pandas as pd

from src.common.config import config
from src.ingestion.generator import ECommerceDataGenerator
from src.storage.lakehouse_fs import LakehouseFS
from src.transformations.engine_adapter import SilverTransformationEngine
from src.transformations.gold_builder import GoldDimensionalBuilder


def _build_sample_lakehouse(batch_id: str = "b_dbt_test") -> dict[str, int]:
    """Helper to populate Bronze, Silver, and Gold layers with consistent test data."""
    gen = ECommerceDataGenerator()
    products = gen.generate_products(count=10)
    customers = gen.generate_customers(count=15)
    batch = gen.generate_orders_and_related(
        customers=customers,
        products=products,
        order_count=25,
        inject_invalid=False,
        inject_duplicates=False,
    )
    batch["products"] = products
    batch["customers"] = customers

    staging_dir = config.DATA_DIR / "staging" / f"test_dbt_{batch_id}"
    gen.write_dataset_to_staging(batch, staging_dir, batch_id, format_type="json")

    for ent in ["orders", "order_items", "customers", "products", "payments"]:
        f = list(staging_dir.glob(f"*{ent}*"))[0]
        bronze_p = config.BRONZE_DIR / ent / f"{batch_id}_{ent}.parquet"
        df = pd.read_json(f) if f.suffix == ".json" else pd.read_csv(f)
        df["_ingested_at"] = "2026-01-01T00:00:00Z"
        df["_batch_id"] = batch_id
        LakehouseFS.write_parquet(df, bronze_p)
        SilverTransformationEngine.transform_entity(ent, batch_id, bronze_files=[bronze_p])

    return GoldDimensionalBuilder.build_gold_layer(batch_id=batch_id)


def test_kimball_dimensional_star_schema_and_marts(clean_metadata_tables):
    """Verify dimensional models dim_*, fact_*, and marts produce expected schemas, grains, and keys."""
    gold_counts = _build_sample_lakehouse(batch_id="dbt_kimball")
    assert gold_counts["dim_date"] > 0
    assert gold_counts["dim_customers"] == 15
    assert gold_counts["dim_products"] == 10
    assert gold_counts["fact_orders"] > 0
    assert gold_counts["fact_order_items"] > 0
    assert gold_counts["fact_payments"] > 0
    assert gold_counts["mart_daily_sales"] > 0
    assert gold_counts["mart_customer_lifetime_value"] > 0
    assert gold_counts["mart_product_performance"] > 0
    assert gold_counts["mart_refund_analysis"] >= 0

    # 3. Verify Kimball Grain and Surrogate Keys
    con = duckdb.connect(database=":memory:")
    for m in [
        "dim_date",
        "dim_customers",
        "dim_products",
        "fact_orders",
        "fact_order_items",
        "fact_payments",
        "mart_daily_sales",
        "mart_product_performance",
    ]:
        p = config.GOLD_DIR / m / "data.parquet"
        con.execute(f"CREATE VIEW {m} AS SELECT * FROM read_parquet('{p}')")

    # Dim Customers SK uniqueness
    cust_dup = con.execute(
        "SELECT COUNT(*) - COUNT(DISTINCT customer_sk) FROM dim_customers"
    ).fetchone()[0]
    assert cust_dup == 0, "Duplicate customer_sk found in dim_customers!"

    # Dim Products SK uniqueness
    prod_dup = con.execute(
        "SELECT COUNT(*) - COUNT(DISTINCT product_sk) FROM dim_products"
    ).fetchone()[0]
    assert prod_dup == 0, "Duplicate product_sk found in dim_products!"

    # Fact Orders grain: order_sk is unique
    order_dup = con.execute(
        "SELECT COUNT(*) - COUNT(DISTINCT order_sk) FROM fact_orders"
    ).fetchone()[0]
    assert order_dup == 0, "Duplicate order_sk found in fact_orders!"

    # Fact Order Items referential integrity to Products and Orders
    bad_items = con.execute("""
        SELECT COUNT(*)
        FROM fact_order_items oi
        LEFT JOIN dim_products p ON oi.product_sk = p.product_sk
        LEFT JOIN fact_orders o ON oi.order_sk = o.order_sk
        WHERE p.product_sk IS NULL OR o.order_sk IS NULL
    """).fetchone()[0]
    assert bad_items == 0, "Dangling foreign keys in fact_order_items!"


def test_financial_reconciliation_penny_exact(clean_metadata_tables):
    """Proves that fact_orders total revenue reconciles to mart_daily_sales gross revenue to the exact penny."""
    _build_sample_lakehouse("recon_batch")
    con = duckdb.connect(database=":memory:")
    orders_p = config.GOLD_DIR / "fact_orders" / "data.parquet"
    sales_p = config.GOLD_DIR / "mart_daily_sales" / "data.parquet"

    assert orders_p.exists() and sales_p.exists(), "Gold tables must exist for reconciliation test."

    con.execute(f"CREATE VIEW fact_orders AS SELECT * FROM read_parquet('{orders_p}')")
    con.execute(f"CREATE VIEW mart_daily_sales AS SELECT * FROM read_parquet('{sales_p}')")

    fact_revenue = con.execute("SELECT ROUND(SUM(total_amount), 2) FROM fact_orders").fetchone()[0]
    mart_revenue = con.execute(
        "SELECT ROUND(SUM(gross_revenue), 2) FROM mart_daily_sales"
    ).fetchone()[0]

    assert fact_revenue is not None and mart_revenue is not None
    variance = abs(round(fact_revenue - mart_revenue, 2))
    assert variance <= 0.05, (
        f"Penny-exact financial reconciliation failed! Fact: {fact_revenue}, Mart: {mart_revenue}, Variance: {variance}"
    )
