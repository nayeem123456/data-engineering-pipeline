#!/usr/bin/env python3
"""
Standalone PySpark Job: Lakehouse Silver Cleansing & Deduplication.
Designed for execution via spark-submit in containerized or cluster environments.
Usage:
    spark-submit --master local[*] spark/jobs/silver_cleaning_job.py --batch-id BATCH_001
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT_DIR))

from src.common.config import config
from src.common.logger import get_logger
from src.transformations.spark_session import SparkSessionFactory
from src.transformations.spark_silver import SparkSilverTransformer

logger = get_logger("spark.jobs.silver_cleaning")


def main():
    parser = argparse.ArgumentParser(description="PySpark Silver Transformation Job")
    parser.add_argument("--batch-id", required=True, help="Ingestion batch identifier")
    args = parser.parse_args()

    logger.info("Starting PySpark Silver Transformation Job for batch %s", args.batch_id)

    bronze_orders = config.BRONZE_DIR / "orders"
    silver_orders = config.SILVER_DIR / "orders"

    if not bronze_orders.exists():
        logger.warning("Bronze orders path does not exist: %s. Exiting cleanly.", bronze_orders)
        sys.exit(0)

    try:
        result = SparkSilverTransformer.process_orders(
            bronze_path=bronze_orders,
            silver_path=silver_orders,
            batch_id=args.batch_id,
        )
        logger.info("PySpark Silver Job succeeded: %s", result)
    finally:
        SparkSessionFactory.stop_session()


if __name__ == "__main__":
    main()
