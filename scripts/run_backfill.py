#!/usr/bin/env python3
"""
CLI Entrypoint for executing historical backfills.
Usage:
    python scripts/run_backfill.py --start-date 2026-01-01 --end-date 2026-01-07
"""

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from src.common.logger import get_logger
from src.orchestration.backfill_runner import BackfillCoordinator

logger = get_logger("scripts.run_backfill")


def main():
    parser = argparse.ArgumentParser(description="Execute Historical Backfill")
    parser.add_argument("--start-date", required=True, help="Backfill start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="Backfill end date (YYYY-MM-DD)")
    parser.add_argument("--entities", nargs="+", default=None, help="Entities to reprocess")

    args = parser.parse_args()

    try:
        res = BackfillCoordinator.execute_backfill(
            start_date=args.start_date,
            end_date=args.end_date,
            entities=args.entities,
        )
        print("\n--- Backfill Summary ---")
        print(f"Backfill ID: {res['backfill_id']}")
        print(f"Status:      {res['status']}")
        print(f"Partitions:  {res['processed_partitions']}")
        print(f"Gold Tables: {res['gold_counts']}")
    except Exception as exc:
        logger.error("Backfill failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
