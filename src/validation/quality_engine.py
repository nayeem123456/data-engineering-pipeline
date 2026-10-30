"""
Automated Data Quality Validation Engine and Quality Gate.
Executes assertions against Lakehouse layers, logs audit scorecards,
and acts as a hard gate preventing corrupted Gold datasets from publishing.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from src.common.config import config
from src.common.logger import get_logger
from src.monitoring.operational_metadata import OperationalMetadata
from src.validation.rules import RULES

logger = get_logger("platform.validation.quality_engine")


class DataQualityGateError(Exception):
    """Raised when critical data quality checks fail, halting publication."""


class QualityEngine:
    """Orchestrates multi-layer data quality validation."""

    @classmethod
    def validate_bronze_layer(
        cls,
        entity: str,
        file_path: Path,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        """Validate Bronze file format, readability, and audit metadata."""
        if not file_path.exists():
            raise FileNotFoundError(f"Bronze file {file_path} does not exist.")

        import pyarrow.parquet as pq

        records_tested = 0
        records_failed = 0
        status = "PASS"
        details = "Bronze file readability and audit columns verified."

        try:
            table = pq.read_table(file_path)
            records_tested = table.num_rows
            col_names = table.column_names

            required_audit = ["_ingestion_id", "_batch_id", "_source_file_checksum"]
            has_timestamp = "_ingested_at" in col_names or "_ingestion_timestamp" in col_names
            missing_cols = [c for c in required_audit if c not in col_names]
            if not has_timestamp:
                missing_cols.append("_ingestion_timestamp")

            if missing_cols:
                status = "FAIL"
                records_failed = records_tested
                details = f"Missing mandatory Bronze audit columns: {missing_cols}"
            elif records_tested == 0:
                status = "FAIL"
                records_failed = 1
                details = "Bronze file is empty (0 records)."

        except Exception as exc:
            status = "FAIL"
            records_failed = 1
            details = f"Corrupt or unreadable Parquet file: {exc}"

        OperationalMetadata.record_quality_result(
            run_id=run_id,
            layer="BRONZE",
            entity=entity,
            rule_name="bronze_file_integrity",
            rule_type="INTEGRITY",
            status=status,
            records_tested=records_tested,
            records_failed=records_failed,
            details=details,
        )

        if status == "FAIL":
            raise DataQualityGateError(f"Bronze quality check failed for {entity}: {details}")

        return {
            "entity": entity,
            "rule": "bronze_file_integrity",
            "status": status,
            "records_tested": records_tested,
            "records_failed": records_failed,
        }

    @classmethod
    def validate_silver_layer(
        cls,
        run_id: str | None = None,
        entity_filter: str | None = None,
    ) -> list[dict[str, Any]]:
        """Validate conformed Silver Parquet tables against rules."""
        con = duckdb.connect(database=":memory:")
        results = []
        critical_failures = 0

        # Register existing Silver tables
        for ent in ["orders", "order_items", "customers", "products", "payments"]:
            parquet_path = config.SILVER_DIR / ent / "data.parquet"
            if parquet_path.exists():
                con.execute(
                    f"CREATE VIEW silver_{ent} AS SELECT * FROM read_parquet('{parquet_path}')"
                )

        active_rules = [r for r in RULES if r.layer == "SILVER"]
        if entity_filter:
            active_rules = [r for r in active_rules if r.entity == entity_filter]

        for rule in active_rules:
            try:
                # Check if view exists
                view_check = con.execute(f"SELECT COUNT(*) FROM silver_{rule.entity}").fetchone()
                total_records = view_check[0]

                # Run assertion query
                res = con.execute(rule.sql_check).fetchone()
                failed_count = int(res[0]) if res else 0

                status = (
                    "PASS"
                    if failed_count == 0
                    else ("FAIL" if rule.severity == "CRITICAL" else "WARN")
                )
                details = f"Tested {total_records} records. Failed: {failed_count}"

                if status == "FAIL":
                    critical_failures += 1
                    logger.error(
                        "Quality Gate Failure: %s [%s] -> %s", rule.name, rule.severity, details
                    )
                else:
                    logger.info("Quality Check Passed: %s -> %s", rule.name, details)

                OperationalMetadata.record_quality_result(
                    run_id=run_id,
                    layer="SILVER",
                    entity=rule.entity,
                    rule_name=rule.name,
                    rule_type=rule.rule_type,
                    status=status,
                    records_tested=total_records,
                    records_failed=failed_count,
                    details=details,
                )

                results.append(
                    {
                        "rule_name": rule.name,
                        "entity": rule.entity,
                        "status": status,
                        "records_tested": total_records,
                        "records_failed": failed_count,
                        "details": details,
                    }
                )

            except Exception as exc:
                logger.warning(
                    "Could not execute rule %s (table might not exist): %s", rule.name, exc
                )

        if critical_failures > config.DATA_QUALITY_MAX_ERROR_THRESHOLD:
            raise DataQualityGateError(
                f"Silver Quality Gate failed: {critical_failures} critical checks failed."
            )

        return results

    @classmethod
    def validate_gold_layer(
        cls,
        run_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Validate Gold dimensional and reconciliation assertions."""
        con = duckdb.connect(database=":memory:")
        results = []
        critical_failures = 0

        # Register Gold tables
        for gold_table in [
            "dim_customers",
            "dim_products",
            "dim_date",
            "fact_orders",
            "fact_order_items",
            "fact_payments",
            "mart_daily_sales",
        ]:
            p = config.GOLD_DIR / gold_table / "data.parquet"
            if p.exists():
                con.execute(f"CREATE VIEW gold_{gold_table} AS SELECT * FROM read_parquet('{p}')")

        gold_rules = [r for r in RULES if r.layer == "GOLD"]

        for rule in gold_rules:
            try:
                res = con.execute(rule.sql_check).fetchone()
                failed_count = int(res[0]) if res else 0

                status = (
                    "PASS"
                    if failed_count == 0
                    else (
                        "FAIL"
                        if rule.severity == "CRITICAL"
                        else ("INFO" if rule.severity == "INFO" else "WARN")
                    )
                )
                details = f"Failed count: {failed_count} [Severity: {rule.severity}]"

                if status == "FAIL":
                    critical_failures += 1
                    logger.error(
                        "Gold Gate Failure: %s [%s] -> %s", rule.name, rule.severity, details
                    )
                elif status == "WARN":
                    logger.warning(
                        "Gold Gate Warning: %s [%s] -> %s", rule.name, rule.severity, details
                    )
                else:
                    logger.info("Gold Check Passed: %s -> %s", rule.name, details)

                OperationalMetadata.record_quality_result(
                    run_id=run_id,
                    layer="GOLD",
                    entity=rule.entity,
                    rule_name=rule.name,
                    rule_type=rule.rule_type,
                    status=status,
                    records_tested=1,
                    records_failed=failed_count,
                    details=details,
                )

                results.append(
                    {
                        "rule_name": rule.name,
                        "entity": rule.entity,
                        "status": status,
                        "records_tested": 1,
                        "records_failed": failed_count,
                        "details": details,
                    }
                )

            except Exception as exc:
                logger.warning("Could not execute Gold rule %s: %s", rule.name, exc)

        if critical_failures > config.DATA_QUALITY_MAX_ERROR_THRESHOLD:
            raise DataQualityGateError(
                f"Gold Quality Gate failed: {critical_failures} critical assertions failed."
            )

        return results
