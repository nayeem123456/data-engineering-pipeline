"""
Quarantine Isolation Manager.
Routes malformed, invalid, or policy-violating records away from production datasets
into persistent quarantine storage and metadata tables for operator inspection.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from src.common.logger import get_logger
from src.monitoring.operational_metadata import OperationalMetadata
from src.storage.lakehouse_fs import LakehouseFS

logger = get_logger("platform.validation.quarantine")


class QuarantineManager:
    """Manages record-level isolation for data lakehouse quality failures."""

    @classmethod
    def quarantine_records(
        cls,
        invalid_rows: list[dict[str, Any]],
        entity: str,
        batch_id: str,
        error_category: str,
        default_error_message: str,
        id_field: str = "order_id",
    ) -> int:
        """
        Persist invalid records into the quarantine Lakehouse layer and metadata store.
        Returns count of quarantined records.
        """
        if not invalid_rows:
            return 0

        quarantine_items = []
        now = datetime.now(UTC).isoformat()

        for row in invalid_rows:
            rec_id = str(row.get(id_field, row.get("_source_record_id", "unknown")))
            msg = row.get("_quarantine_reason", default_error_message)

            # Clean internal temporary tracking fields from raw payload
            clean_row = {k: v for k, v in row.items() if not k.startswith("_quarantine")}
            raw_payload = json.dumps(clean_row, default=str)

            # Record in operational database
            OperationalMetadata.record_quarantine_item(
                batch_id=batch_id,
                entity=entity,
                source_record_id=rec_id,
                raw_payload=raw_payload,
                error_category=error_category,
                error_message=msg,
            )

            quarantine_items.append(
                {
                    "entity": entity,
                    "batch_id": batch_id,
                    "source_record_id": rec_id,
                    "error_category": error_category,
                    "error_message": msg,
                    "raw_payload": raw_payload,
                    "quarantined_at": now,
                }
            )

        # Save to partitioned quarantine Parquet file
        LakehouseFS.write_quarantine_records(
            records=quarantine_items,
            entity=entity,
            batch_id=batch_id,
        )

        logger.warning(
            "Quarantined %d invalid records for entity '%s' in batch '%s' [Category: %s]",
            len(quarantine_items),
            entity,
            batch_id,
            error_category,
        )

        return len(quarantine_items)
