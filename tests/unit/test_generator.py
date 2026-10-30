"""
Unit Tests for E-Commerce Synthetic Generator.
"""

from __future__ import annotations

from src.ingestion.generator import ECommerceDataGenerator


def test_generate_products():
    gen = ECommerceDataGenerator()
    products = gen.generate_products(count=20)
    assert len(products) == 20
    first = products[0]
    assert first["product_id"].startswith("PROD-")
    assert first["retail_price"] > 0
    assert first["cost_price"] > 0
    assert first["price_band"] in ["BUDGET", "MID_RANGE", "PREMIUM", "LUXURY"]


def test_generate_customers():
    gen = ECommerceDataGenerator()
    customers = gen.generate_customers(count=25)
    assert len(customers) == 25
    first = customers[0]
    assert first["customer_id"].startswith("CUST-")
    assert "@" in first["email"]
    assert first["customer_segment"] in ["REGULAR", "VIP", "NEW", "DORMANT"]


def test_generate_orders_with_edge_cases():
    gen = ECommerceDataGenerator()
    products = gen.generate_products(count=10)
    customers = gen.generate_customers(count=10)

    dataset = gen.generate_orders_and_related(
        customers=customers,
        products=products,
        order_count=15,
        inject_invalid=True,
        inject_duplicates=True,
    )

    orders = dataset["orders"]
    items = dataset["order_items"]
    payments = dataset["payments"]
    events = dataset["events"]

    assert len(orders) >= 15
    assert len(items) >= 15
    assert len(payments) >= 15
    assert len(events) >= 15

    # Verify injected invalid records are present
    bad_orders = [
        o for o in orders if o["total_amount"] < 0 or o["order_status"] == "INVALID_STATUS_CODE"
    ]
    assert len(bad_orders) > 0
