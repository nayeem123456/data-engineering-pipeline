"""
Configurable E-Commerce Data Generator.
Produces realistic synthetic datasets for initial historical loads, incremental batches,
and controlled edge cases (duplicates, late arrivals, and invalid records).
"""

from __future__ import annotations

import random
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from src.common.logger import get_logger

logger = get_logger("platform.ingestion.generator")

# Deterministic seed for reproducible tests
random.seed(42)

CATEGORIES = [
    (
        "Electronics",
        [
            "Wireless Headphones",
            "Smartwatch Pro",
            "4K Ultra Monitor",
            "USB-C Hub",
            "Mechanical Keyboard",
        ],
    ),
    (
        "Apparel",
        [
            "Organic Cotton Hoodie",
            "Trail Running Shoes",
            "Denim Jacket",
            "Merino Wool Socks",
            "Performance Tee",
        ],
    ),
    (
        "Home & Kitchen",
        [
            "Espresso Machine",
            "Cast Iron Skillet",
            "Robot Vacuum",
            "Ceramic Dinnerware",
            "Air Purifier",
        ],
    ),
    (
        "Books",
        [
            "Designing Data-Intensive Applications",
            "Building Microservices",
            "Modern Data Platform Architecture",
            "Clean Code",
            "Python Crash Course",
        ],
    ),
    (
        "Sports",
        [
            "Yoga Mat Eco",
            "Adjustable Dumbbells",
            "Insulated Hydro Flask",
            "Cycling Helmet",
            "Resistance Bands Set",
        ],
    ),
]

CITIES_STATES = [
    ("Seattle", "WA"),
    ("Austin", "TX"),
    ("San Francisco", "CA"),
    ("New York", "NY"),
    ("Denver", "CO"),
    ("Chicago", "IL"),
    ("Boston", "MA"),
    ("Atlanta", "GA"),
    ("Miami", "FL"),
]

ORDER_STATUSES = ["COMPLETED", "SHIPPED", "PROCESSING", "CANCELLED", "REFUNDED"]
PAYMENT_METHODS = ["CREDIT_CARD", "PAYPAL", "APPLE_PAY", "BANK_TRANSFER"]
EVENT_TYPES = ["PAGE_VIEW", "PRODUCT_VIEW", "ADD_TO_CART", "CHECKOUT_STARTED", "PURCHASE_COMPLETED"]


class ECommerceDataGenerator:
    """Generates synthetic e-commerce Lakehouse source data."""

    def __init__(self, base_date: datetime | None = None):
        self.base_date = base_date or (datetime.now(UTC) - timedelta(days=14))

    def generate_products(self, count: int = 50) -> list[dict[str, Any]]:
        """Generate product catalog records."""
        products = []
        for i in range(1, count + 1):
            category, item_names = random.choice(CATEGORIES)
            base_name = random.choice(item_names)
            product_name = f"{base_name} #{i}"
            cost_price = round(random.uniform(10.0, 300.0), 2)
            margin_multiplier = random.uniform(1.3, 2.2)
            retail_price = round(cost_price * margin_multiplier, 2)

            if retail_price < 50:
                price_band = "BUDGET"
            elif retail_price < 150:
                price_band = "MID_RANGE"
            elif retail_price < 400:
                price_band = "PREMIUM"
            else:
                price_band = "LUXURY"

            created_at = self.base_date - timedelta(days=random.randint(30, 90))
            updated_at = created_at + timedelta(days=random.randint(0, 20))

            products.append(
                {
                    "product_id": f"PROD-{i:04d}",
                    "product_name": product_name,
                    "category": category,
                    "brand": "AeroCore Tech",
                    "cost_price": cost_price,
                    "retail_price": retail_price,
                    "price_band": price_band,
                    "status": "ACTIVE",
                    "created_at": created_at.isoformat(),
                    "updated_at": updated_at.isoformat(),
                }
            )
        return products

    def generate_customers(self, count: int = 100) -> list[dict[str, Any]]:
        """Generate customer demographic profiles."""
        customers = []
        first_names = [
            "Alex",
            "Jordan",
            "Taylor",
            "Morgan",
            "Sam",
            "Chris",
            "Pat",
            "Riley",
            "Cameron",
            "Avery",
        ]
        last_names = [
            "Smith",
            "Johnson",
            "Williams",
            "Brown",
            "Jones",
            "Garcia",
            "Miller",
            "Davis",
            "Martinez",
            "Hernandez",
        ]

        for i in range(1, count + 1):
            fn = random.choice(first_names)
            ln = random.choice(last_names)
            city, state = random.choice(CITIES_STATES)
            signup_date = (self.base_date - timedelta(days=random.randint(10, 180))).strftime(
                "%Y-%m-%d"
            )
            segment = random.choices(
                ["REGULAR", "VIP", "NEW", "DORMANT"], weights=[0.55, 0.15, 0.20, 0.10]
            )[0]

            created_at = datetime.fromisoformat(f"{signup_date}T00:00:00+00:00")
            updated_at = created_at + timedelta(days=random.randint(0, 10))

            customers.append(
                {
                    "customer_id": f"CUST-{i:05d}",
                    "first_name": fn,
                    "last_name": ln,
                    "email": f"{fn.lower()}.{ln.lower()}{i}@example.com",
                    "city": city,
                    "state": state,
                    "country": "USA",
                    "customer_segment": segment,
                    "signup_date": signup_date,
                    "is_active": True,
                    "created_at": created_at.isoformat(),
                    "updated_at": updated_at.isoformat(),
                }
            )
        return customers

    def generate_orders_and_related(
        self,
        customers: list[dict[str, Any]],
        products: list[dict[str, Any]],
        order_count: int = 200,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        inject_invalid: bool = False,
        inject_duplicates: bool = False,
    ) -> dict[str, list[dict[str, Any]]]:
        """Generate orders, line items, payments, and clickstream events."""
        start = start_date or self.base_date
        end = end_date or (start + timedelta(days=7))

        orders = []
        order_items = []
        payments = []
        events = []

        for _i in range(1, order_count + 1):
            order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
            customer = random.choice(customers)
            cust_id = customer["customer_id"]

            # Timestamp between start and end
            delta_seconds = int((end - start).total_seconds())
            rand_seconds = random.randint(0, max(delta_seconds, 1))
            created_at = start + timedelta(seconds=rand_seconds)
            updated_at = created_at + timedelta(minutes=random.randint(5, 120))

            # Pick 1 to 4 items
            chosen_products = random.sample(products, k=random.randint(1, min(4, len(products))))
            subtotal = 0.0
            order_lines = []

            for prod in chosen_products:
                qty = random.randint(1, 3)
                price = float(prod["retail_price"])
                total_line = round(qty * price, 2)
                subtotal += total_line

                order_lines.append(
                    {
                        "order_id": order_id,
                        "product_id": prod["product_id"],
                        "quantity": qty,
                        "unit_price": price,
                        "total_price": total_line,
                        "created_at": created_at.isoformat(),
                    }
                )

            subtotal = round(subtotal, 2)
            tax = round(subtotal * 0.08, 2)
            shipping = 0.0 if subtotal > 100 else 9.99
            discount = round(subtotal * 0.1, 2) if random.random() < 0.25 else 0.0
            total_amount = round(subtotal + tax + shipping - discount, 2)
            status = random.choices(ORDER_STATUSES, weights=[0.70, 0.15, 0.08, 0.04, 0.03])[0]

            order_record = {
                "order_id": order_id,
                "customer_id": cust_id,
                "order_status": status,
                "currency": "USD",
                "subtotal_amount": subtotal,
                "tax_amount": tax,
                "shipping_amount": shipping,
                "discount_amount": discount,
                "total_amount": total_amount,
                "created_at": created_at.isoformat(),
                "updated_at": updated_at.isoformat(),
            }
            orders.append(order_record)
            order_items.extend(order_lines)

            # Generate Payment
            pay_id = f"PAY-{uuid.uuid4().hex[:8].upper()}"
            pay_method = random.choice(PAYMENT_METHODS)
            pay_status = (
                "CAPTURED"
                if status in ["COMPLETED", "SHIPPED", "PROCESSING"]
                else ("REFUNDED" if status == "REFUNDED" else "FAILED")
            )

            payments.append(
                {
                    "payment_id": pay_id,
                    "order_id": order_id,
                    "payment_method": pay_method,
                    "payment_status": pay_status,
                    "amount": total_amount,
                    "is_refund": (status == "REFUNDED"),
                    "created_at": (created_at + timedelta(minutes=2)).isoformat(),
                }
            )

            # Generate Clickstream session events for this order
            session_id = f"SESS-{uuid.uuid4().hex[:8]}"
            events.append(
                {
                    "event_id": f"EVT-{uuid.uuid4().hex[:8]}",
                    "customer_id": cust_id,
                    "session_id": session_id,
                    "event_type": "PAGE_VIEW",
                    "product_id": None,
                    "timestamp": (created_at - timedelta(minutes=10)).isoformat(),
                }
            )
            events.append(
                {
                    "event_id": f"EVT-{uuid.uuid4().hex[:8]}",
                    "customer_id": cust_id,
                    "session_id": session_id,
                    "event_type": "CHECKOUT_STARTED",
                    "product_id": chosen_products[0]["product_id"],
                    "timestamp": (created_at - timedelta(minutes=2)).isoformat(),
                }
            )
            events.append(
                {
                    "event_id": f"EVT-{uuid.uuid4().hex[:8]}",
                    "customer_id": cust_id,
                    "session_id": session_id,
                    "event_type": "PURCHASE_COMPLETED",
                    "product_id": chosen_products[0]["product_id"],
                    "timestamp": created_at.isoformat(),
                }
            )

        # Inject controlled invalid records for quarantine validation
        if inject_invalid:
            # Order with negative total amount
            bad_order_1 = orders[0].copy()
            bad_order_1["order_id"] = f"BAD-ORD-{uuid.uuid4().hex[:6]}"
            bad_order_1["total_amount"] = -150.00
            orders.append(bad_order_1)

            # Order with invalid status
            bad_order_2 = orders[1].copy()
            bad_order_2["order_id"] = f"BAD-ORD-{uuid.uuid4().hex[:6]}"
            bad_order_2["order_status"] = "INVALID_STATUS_CODE"
            orders.append(bad_order_2)

        # Inject duplicate records with newer timestamp for deduplication test
        if inject_duplicates and orders:
            dup_order = orders[0].copy()
            # Simulate a status update to SHIPPED
            dup_order["order_status"] = "SHIPPED"
            orig_updated = datetime.fromisoformat(dup_order["updated_at"])
            dup_order["updated_at"] = (orig_updated + timedelta(hours=2)).isoformat()
            orders.append(dup_order)

        return {
            "orders": orders,
            "order_items": order_items,
            "payments": payments,
            "events": events,
        }

    def write_dataset_to_staging(
        self,
        dataset: dict[str, list[dict[str, Any]]],
        staging_dir: Path,
        batch_id: str,
        format_type: str = "json",
    ) -> dict[str, Path]:
        """Save generated datasets to staging files (CSV or JSON)."""
        staging_dir.mkdir(parents=True, exist_ok=True)
        file_paths = {}

        for entity, records in dataset.items():
            df = pd.DataFrame(records)
            if format_type.lower() == "csv":
                file_path = staging_dir / f"{entity}_{batch_id}.csv"
                df.to_csv(file_path, index=False)
            else:
                file_path = staging_dir / f"{entity}_{batch_id}.json"
                df.to_json(file_path, orient="records", indent=2)

            file_paths[entity] = file_path
            logger.info("Wrote %d records to staging %s", len(records), file_path)

        return file_paths
