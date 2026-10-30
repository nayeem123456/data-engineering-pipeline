"""
Integration Tests for Silver Transformations across all Entities.
"""

from __future__ import annotations

import pandas as pd

from src.common.config import config
from src.storage.lakehouse_fs import LakehouseFS
from src.transformations.engine_adapter import SilverTransformationEngine


def test_silver_transform_all_entities(clean_metadata_tables):
    # 1. Prepare Bronze Products
    prod_df = pd.DataFrame(
        [
            {
                "product_id": "PROD-INT-1",
                "product_name": "Premium Keyboard",
                "category": "Electronics",
                "brand": "TechCorp",
                "cost_price": "75.00",
                "retail_price": "149.99",
                "price_band": "MID_RANGE",
                "status": "ACTIVE",
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
                "_ingested_at": "2026-01-01T00:00:00Z",
                "_batch_id": "b_int",
            }
        ]
    )
    LakehouseFS.write_bronze_batch(prod_df, "products", "prod_b1", "2026-01-01")

    # 2. Prepare Bronze Customers
    cust_df = pd.DataFrame(
        [
            {
                "customer_id": "CUST-INT-1",
                "first_name": "Alice",
                "last_name": "Smith",
                "email": "alice.smith@example.com",
                "city": "Seattle",
                "state": "WA",
                "country": "USA",
                "customer_segment": "VIP",
                "signup_date": "2026-01-01",
                "is_active": "true",
                "created_at": "2026-01-01T00:00:00Z",
                "updated_at": "2026-01-01T00:00:00Z",
                "_ingested_at": "2026-01-01T00:00:00Z",
                "_batch_id": "b_int",
            }
        ]
    )
    LakehouseFS.write_bronze_batch(cust_df, "customers", "cust_b1", "2026-01-01")

    # 3. Prepare Bronze Orders
    ord_df = pd.DataFrame(
        [
            {
                "order_id": "ORD-INT-1",
                "customer_id": "CUST-INT-1",
                "order_status": "COMPLETED",
                "currency": "usd",
                "subtotal_amount": "149.99",
                "tax_amount": "12.00",
                "shipping_amount": "0.00",
                "discount_amount": "0.00",
                "total_amount": "161.99",
                "created_at": "2026-01-02T10:00:00Z",
                "updated_at": "2026-01-02T10:00:00Z",
                "_ingested_at": "2026-01-02T10:00:00Z",
                "_batch_id": "b_int",
            }
        ]
    )
    LakehouseFS.write_bronze_batch(ord_df, "orders", "ord_b1", "2026-01-02")

    # 4. Prepare Bronze Order Items
    item_df = pd.DataFrame(
        [
            {
                "order_id": "ORD-INT-1",
                "product_id": "PROD-INT-1",
                "quantity": "1",
                "unit_price": "149.99",
                "created_at": "2026-01-02T10:00:00Z",
                "_ingested_at": "2026-01-02T10:00:00Z",
                "_batch_id": "b_int",
            }
        ]
    )
    LakehouseFS.write_bronze_batch(item_df, "order_items", "item_b1", "2026-01-02")

    # 5. Prepare Bronze Payments
    pay_df = pd.DataFrame(
        [
            {
                "payment_id": "PAY-INT-1",
                "order_id": "ORD-INT-1",
                "payment_method": "CREDIT_CARD",
                "payment_status": "CAPTURED",
                "amount": "161.99",
                "is_refund": "false",
                "created_at": "2026-01-02T10:02:00Z",
                "_ingested_at": "2026-01-02T10:02:00Z",
                "_batch_id": "b_int",
            }
        ]
    )
    LakehouseFS.write_bronze_batch(pay_df, "payments", "pay_b1", "2026-01-02")

    # Execute transformations for each entity
    for ent in ["products", "customers", "orders", "order_items", "payments"]:
        res = SilverTransformationEngine.transform_entity(entity=ent, batch_id="int_test")
        assert res["rows_out"] == 1
        assert res["rows_rejected"] == 0

        # Verify silver parquet is readable
        silver_file = config.SILVER_DIR / ent / "data.parquet"
        assert silver_file.exists()
        out_df = LakehouseFS.read_parquet(silver_file)
        assert len(out_df) == 1
