"""
Historical Backfill and Reprocessing Coordinator.
Enables operators to reprocess historical intervals, reconstruct Silver and Gold partitions,
prevent duplicate records, and track backfill execution status.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import text

from src.common.db import db_manager
from src.common.logger import get_logger
from src.transformations.engine_adapter import SilverTransformationEngine
from src.transformations.gold_builder import GoldDimensionalBuilder
from src.validation.quality_engine import QualityEngine

logger = get_logger("platform.orchestration.backfill_runner")


class BackfillCoordinator:
    """Manages historical backfills and partition-level reprocessing."""

    @classmethod
    def execute_backfill(
        cls,
        start_date: str,
        end_date: str,
        entities: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Execute historical reprocessing for specified interval [start_date, end_date].
        """
        backfill_id = f"bf_{uuid.uuid4().hex[:10]}"
        now = datetime.now(UTC)
        target_entities = entities or ["orders", "order_items", "customers", "products", "payments"]

        table = "metadata_backfill_runs" if db_manager.is_sqlite() else "metadata.backfill_runs"

        # Record backfill initialization
        init_query = text(f"""
            INSERT INTO {table} (backfill_id, entity, start_date, end_date, status, processed_partitions, created_at)
            VALUES (:bid, :entity, :sdate, :edate, 'IN_PROGRESS', '', :now)
        """)
        with db_manager.get_session() as session:
            session.execute(
                init_query,
                {
                    "bid": backfill_id,
                    "entity": ",".join(target_entities),
                    "sdate": start_date,
                    "edate": end_date,
                    "now": now,
                },
            )

        processed_partitions = []
        try:
            if start_date > end_date:
                raise ValueError(
                    f"Invalid backfill range: start_date {start_date} is after end_date {end_date}"
                )

            valid_entities = {"orders", "order_items", "customers", "products", "payments"}
            for ent in target_entities:
                if ent not in valid_entities:
                    raise ValueError(
                        f"Unknown backfill entity '{ent}'. Must be one of {valid_entities}"
                    )
            # 1. Reprocess Silver partitions for target entities
            for ent in target_entities:
                res = SilverTransformationEngine.transform_entity(
                    entity=ent,
                    batch_id=backfill_id,
                )
                processed_partitions.append(f"{ent}:{res['rows_out']} rows")

            # 2. Run Silver Quality Gates
            QualityEngine.validate_silver_layer(run_id=backfill_id)

            # 3. Rebuild Gold Facts and Marts
            gold_counts = GoldDimensionalBuilder.build_gold_layer(batch_id=backfill_id)

            # 4. Run Gold Quality Gates
            QualityEngine.validate_gold_layer(run_id=backfill_id)

            # 5. Sync to serving database
            GoldDimensionalBuilder.sync_to_serving_database()

            # Record success
            completed_at = datetime.now(UTC)
            update_query = text(f"""
                UPDATE {table}
                SET status = 'COMPLETED', processed_partitions = :parts, completed_at = :end
                WHERE backfill_id = :bid
            """)
            with db_manager.get_session() as session:
                session.execute(
                    update_query,
                    {
                        "parts": "; ".join(processed_partitions),
                        "end": completed_at,
                        "bid": backfill_id,
                    },
                )

            logger.info("Backfill %s completed successfully.", backfill_id)
            return {
                "backfill_id": backfill_id,
                "status": "COMPLETED",
                "processed_partitions": processed_partitions,
                "gold_counts": gold_counts,
            }

        except Exception as exc:
            logger.error("Backfill %s failed: %s", backfill_id, exc)
            completed_at = datetime.now(UTC)
            update_fail_query = text(f"""
                UPDATE {table}
                SET status = 'FAILED', completed_at = :end
                WHERE backfill_id = :bid
            """)
            with db_manager.get_session() as session:
                session.execute(update_fail_query, {"end": completed_at, "bid": backfill_id})
            raise exc
