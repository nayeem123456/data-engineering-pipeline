"""
Stateful Watermark and Checkpoint Management.
Enables incremental extraction, late-arriving event processing, and recovery.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import text

from src.common.config import config
from src.common.db import db_manager
from src.common.logger import get_logger

logger = get_logger("platform.storage.watermark_manager")


class WatermarkManager:
    """Manages persistent watermarks for stateful incremental pipeline execution."""

    @classmethod
    def get_watermark(cls, entity_name: str) -> str | None:
        """Fetch current high watermark value for an entity."""
        query = text("SELECT watermark_value FROM metadata_watermarks WHERE entity_name = :entity")
        if not db_manager.is_sqlite():
            query = text(
                "SELECT watermark_value FROM metadata.watermarks WHERE entity_name = :entity"
            )

        try:
            with db_manager.get_session() as session:
                result = session.execute(query, {"entity": entity_name}).fetchone()
                if result:
                    return str(result[0])
                return None
        except Exception as exc:
            logger.warning(
                "Could not read watermark for %s (table might not exist yet): %s", entity_name, exc
            )
            return None

    @classmethod
    def get_extraction_window(
        cls,
        entity_name: str,
        lookback_hours: int | None = None,
    ) -> tuple[datetime, datetime]:
        """
        Calculate incremental extraction window [window_start, window_end].
        Incorporates late-arriving data grace period.
        """
        now = datetime.now(UTC)
        grace_hours = (
            lookback_hours
            if lookback_hours is not None
            else config.LATE_ARRIVING_GRACE_PERIOD_HOURS
        )
        watermark_val = cls.get_watermark(entity_name)

        if watermark_val:
            try:
                wm_dt = datetime.fromisoformat(watermark_val)
                # Overlap extraction window with grace period to catch late updates
                window_start = wm_dt - timedelta(hours=grace_hours)
            except Exception:
                window_start = now - timedelta(days=30)
        else:
            # Cold start: look back 30 days
            window_start = now - timedelta(days=30)

        window_end = now
        logger.info(
            "Extraction window for %s: %s -> %s (grace period: %d hours)",
            entity_name,
            window_start.isoformat(),
            window_end.isoformat(),
            grace_hours,
        )
        return window_start, window_end

    @classmethod
    def update_watermark(
        cls,
        entity_name: str,
        watermark_value: str,
        batch_id: str,
    ) -> None:
        """
        Atomically update watermark after a successful pipeline run.
        Ensures checkpoint is only advanced when quality gates succeed.
        """
        now = datetime.now(UTC)
        if db_manager.is_sqlite():
            upsert_query = text("""
                INSERT INTO metadata_watermarks (entity_name, watermark_type, watermark_value, last_batch_id, updated_at)
                VALUES (:entity, 'TIMESTAMP', :wm_val, :batch_id, :now)
                ON CONFLICT(entity_name) DO UPDATE SET
                    watermark_value = excluded.watermark_value,
                    last_batch_id = excluded.last_batch_id,
                    updated_at = excluded.updated_at
            """)
        else:
            upsert_query = text("""
                INSERT INTO metadata.watermarks (entity_name, watermark_type, watermark_value, last_batch_id, updated_at)
                VALUES (:entity, 'TIMESTAMP', :wm_val, :batch_id, :now)
                ON CONFLICT(entity_name) DO UPDATE SET
                    watermark_value = EXCLUDED.watermark_value,
                    last_batch_id = EXCLUDED.last_batch_id,
                    updated_at = EXCLUDED.updated_at
            """)

        with db_manager.get_session() as session:
            session.execute(
                upsert_query,
                {
                    "entity": entity_name,
                    "wm_val": watermark_value,
                    "batch_id": batch_id,
                    "now": now,
                },
            )
        logger.info(
            "Advanced watermark for %s to %s (batch %s)", entity_name, watermark_value, batch_id
        )
