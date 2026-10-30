#!/usr/bin/env python3
"""
CLI Utility to generate mock e-commerce datasets into samples/ or staging/
Usage:
    python scripts/generate_mock_data.py --orders 200 --format csv
"""

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from src.common.logger import get_logger
from src.ingestion.generator import ECommerceDataGenerator

logger = get_logger("scripts.generate_mock_data")


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce data files")
    parser.add_argument("--orders", type=int, default=200, help="Order count")
    parser.add_argument("--customers", type=int, default=50, help="Customer count")
    parser.add_argument("--products", type=int, default=30, help="Product count")
    parser.add_argument(
        "--format", choices=["json", "csv"], default="csv", help="Output file format"
    )
    parser.add_argument("--output-dir", type=str, default="data/samples", help="Destination folder")
    parser.add_argument("--inject-invalid", action="store_true", help="Inject bad records")

    args = parser.parse_args()

    dest_dir = ROOT_DIR / args.output_dir
    gen = ECommerceDataGenerator()
    prods = gen.generate_products(count=args.products)
    custs = gen.generate_customers(count=args.customers)
    dataset = gen.generate_orders_and_related(
        customers=custs,
        products=prods,
        order_count=args.orders,
        inject_invalid=args.inject_invalid,
    )
    dataset["products"] = prods
    dataset["customers"] = custs

    files = gen.write_dataset_to_staging(
        dataset=dataset,
        staging_dir=dest_dir,
        batch_id="sample_export",
        format_type=args.format,
    )

    print("\n--- Mock Data Generation Complete ---")
    for entity, path in files.items():
        print(f"  {entity:12}: {path}")


if __name__ == "__main__":
    main()
