"""
Schema Change and Evolution Detection Engine.
Identifies compatible vs breaking schema alterations, records schema audit events,
and guards against silent data corruption.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

import pandas as pd
from sqlalchemy import text

from src.common.config import config
from src.common.db import db_manager
from src.common.logger import get_logger

logger = get_logger("platform.transformations.schema_evolution")


class SchemaEvolutionManager:
    """Detects and classifies schema changes across pipeline executions."""

    _contracts: dict[str, Any] | None = None

    @classmethod
    def load_contracts(cls) -> dict[str, Any]:
        """Load Silver contracts JSON."""
        if cls._contracts is None:
            contract_path = config.PROJECT_ROOT / "schemas" / "silver_schemas.json"
            if contract_path.exists():
                with open(contract_path, encoding="utf-8") as f:
                    cls._contracts = json.load(f)
            else:
                cls._contracts = {"entities": {}}
        return cls._contracts

    @classmethod
    def evaluate_schema(
        cls,
        entity: str,
        df: pd.DataFrame,
        layer: str = "SILVER",
    ) -> tuple[bool, list[dict[str, Any]]]:
        """
        Evaluate incoming DataFrame columns against contract.
        Returns: (is_compatible, list_of_detected_changes)
        """
        contracts = cls.load_contracts().get("entities", {})
        entity_contract = contracts.get(entity)

        if not entity_contract:
            logger.info("No contract registered for entity '%s'. Assuming compatible.", entity)
            return True, []

        expected_columns = entity_contract.get("columns", {})
        incoming_columns = set(df.columns)
        changes = []
        is_compatible = True

        # Check for missing required columns (Breaking)
        for col_name, specs in expected_columns.items():
            is_required = not specs.get("nullable", True) and specs.get("default") is None
            if is_required and col_name not in incoming_columns:
                change = {
                    "entity": entity,
                    "layer": layer,
                    "change_type": "MISSING_REQUIRED_COLUMN",
                    "column": col_name,
                    "details": f"Required column '{col_name}' is missing from incoming data.",
                    "is_compatible": False,
                }
                changes.append(change)
                is_compatible = False
                logger.error("Breaking schema violation: %s", change["details"])

        # Check for new optional columns (Compatible)
        for col_name in incoming_columns:
            if not col_name.startswith("_") and col_name not in expected_columns:
                change = {
                    "entity": entity,
                    "layer": layer,
                    "change_type": "NEW_OPTIONAL_COLUMN",
                    "column": col_name,
                    "details": f"New column '{col_name}' detected. Accommodating via controlled evolution.",
                    "is_compatible": True,
                }
                changes.append(change)
                logger.info("Compatible schema evolution: %s", change["details"])

        # Record changes in operational metadata
        for change in changes:
            cls.record_event(
                entity=entity,
                layer=layer,
                change_type=change["change_type"],
                prev_schema=json.dumps(list(expected_columns.keys())),
                curr_schema=json.dumps(list(incoming_columns)),
                is_compatible=change["is_compatible"],
            )

        return is_compatible, changes

    @classmethod
    def record_event(
        cls,
        entity: str,
        layer: str,
        change_type: str,
        prev_schema: str,
        curr_schema: str,
        is_compatible: bool,
    ) -> None:
        """Persist schema event in metadata table."""
        table = (
            "metadata_schema_change_events"
            if db_manager.is_sqlite()
            else "metadata.schema_change_events"
        )
        event_id = f"sch_{uuid.uuid4().hex[:12]}"
        now = datetime.now(UTC)
        compat_val = 1 if is_compatible else 0

        query = text(f"""
            INSERT INTO {table} (event_id, entity, layer, change_type, previous_schema, current_schema, is_compatible, detected_at)
            VALUES (:eid, :entity, :layer, :ctype, :prev, :curr, :compat, :now)
        """)
        try:
            with db_manager.get_session() as session:
                session.execute(
                    query,
                    {
                        "eid": event_id,
                        "entity": entity,
                        "layer": layer,
                        "ctype": change_type,
                        "prev": prev_schema,
                        "curr": curr_schema,
                        "compat": compat_val,
                        "now": now,
                    },
                )
        except Exception as exc:
            logger.warning("Could not log schema event: %s", exc)
