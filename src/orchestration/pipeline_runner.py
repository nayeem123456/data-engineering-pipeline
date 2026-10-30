"""
End-to-End Lakehouse Pipeline Orchestrator.
Coordinates ingestion, Bronze storage, Silver cleansing, quality gates,
Gold dimensional modeling, PostgreSQL serving publication, and operational telemetry.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.common.config import config
from src.common.logger import get_logger
from src.ingestion.generator import ECommerceDataGenerator
from src.ingestion.raw_ingester import RawIngester
from src.monitoring.operational_metadata import OperationalMetadata
from src.storage.watermark_manager import WatermarkManager
from src.transformations.engine_adapter import SilverTransformationEngine
from src.transformations.gold_builder import GoldDimensionalBuilder
from src.validation.quality_engine import QualityEngine

logger = get_logger("platform.orchestration.pipeline_runner")


class LakehousePipelineRunner:
    """Master controller executing the Medallion ETL/ELT workflow."""

    def __init__(self, run_id: str | None = None):
        self.run_id = (
            run_id or f"run_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        )
        self.entities = ["orders", "order_items", "customers", "products", "payments"]

    def run_pipeline(
        self,
        batch_type: str = "INCREMENTAL",
        generate_sample_data: bool = True,
        sample_order_count: int = 150,
        inject_invalid: bool = False,
        inject_duplicates: bool = False,
        staging_files: dict[str, Path] | None = None,
    ) -> dict[str, Any]:
        """Execute end-to-end Lakehouse pipeline."""
        logger.info("=================================================================")
        logger.info("STARTING LAKEHOUSE PIPELINE RUN: %s (Type: %s)", self.run_id, batch_type)
        logger.info("=================================================================")

        config.ensure_directories()
        OperationalMetadata.start_pipeline_run(
            run_id=self.run_id,
            pipeline_name="ecommerce_lakehouse_pipeline",
            config_payload={
                "batch_type": batch_type,
                "sample_order_count": sample_order_count,
                "inject_invalid": inject_invalid,
                "inject_duplicates": inject_duplicates,
            },
        )

        pipeline_metrics: dict[str, Any] = {
            "run_id": self.run_id,
            "status": "RUNNING",
            "bronze_rows": 0,
            "silver_rows": 0,
            "quarantined_rows": 0,
            "gold_rows": {},
        }

        try:
            # ------------------------------------------------------------------
            # Stage 1: Data Staging / Generation
            # ------------------------------------------------------------------
            datetime.now(UTC)
            input_files = {}

            if staging_files is not None:
                input_files = staging_files
            elif generate_sample_data:
                logger.info("Generating realistic e-commerce synthetic data batch...")
                gen = ECommerceDataGenerator()
                products = gen.generate_products(count=30)
                customers = gen.generate_customers(count=60)
                batch_data = gen.generate_orders_and_related(
                    customers=customers,
                    products=products,
                    order_count=sample_order_count,
                    inject_invalid=inject_invalid,
                    inject_duplicates=inject_duplicates,
                )
                batch_data["products"] = products
                batch_data["customers"] = customers

                staging_dir = config.DATA_DIR / "staging" / self.run_id
                input_files = gen.write_dataset_to_staging(
                    dataset=batch_data,
                    staging_dir=staging_dir,
                    batch_id=self.run_id,
                    format_type="json",
                )
            else:
                staging_base = config.DATA_DIR / "staging"
                if staging_base.exists():
                    for f in staging_base.glob("**/*.*"):
                        if f.suffix in [".json", ".csv"]:
                            for ent in self.entities:
                                if ent in f.name and ent not in input_files:
                                    input_files[ent] = f

            # ------------------------------------------------------------------
            # Stage 2: Bronze Ingestion
            # ------------------------------------------------------------------
            logger.info("--- Stage 2: Ingesting into Bronze Parquet Lakehouse ---")
            t_bronze_start = datetime.now(UTC)
            ingest_result = RawIngester.ingest_batch(
                files=input_files,
                source_name="ecommerce_platform",
                batch_id=self.run_id,
                batch_type=batch_type,
            )
            pipeline_metrics["bronze_rows"] = ingest_result["total_rows"]
            OperationalMetadata.record_task_run(
                run_id=self.run_id,
                task_name="bronze_ingestion",
                status="SUCCESS",
                start_time=t_bronze_start,
                end_time=datetime.now(UTC),
                rows_in=ingest_result["total_rows"],
                rows_out=ingest_result["total_rows"],
            )

            # ------------------------------------------------------------------
            # Stage 3: Bronze Quality Checks
            # ------------------------------------------------------------------
            for res in ingest_result["results"]:
                if res["target_path"]:
                    QualityEngine.validate_bronze_layer(
                        entity=res["entity"],
                        file_path=Path(res["target_path"]),
                        run_id=self.run_id,
                    )

            # ------------------------------------------------------------------
            # Stage 4: Silver Transformation, Deduplication & Quarantine
            # ------------------------------------------------------------------
            logger.info("--- Stage 4: Cleansing & Conforming into Silver Layer ---")
            datetime.now(UTC)
            total_silver_out = 0
            total_quarantine = 0

            for entity in self.entities:
                t_ent_start = datetime.now(UTC)
                transform_res = SilverTransformationEngine.transform_entity(
                    entity=entity,
                    batch_id=self.run_id,
                )
                total_silver_out += transform_res["rows_out"]
                total_quarantine += transform_res["rows_rejected"]

                OperationalMetadata.record_task_run(
                    run_id=self.run_id,
                    task_name=f"silver_transform_{entity}",
                    status="SUCCESS",
                    start_time=t_ent_start,
                    end_time=datetime.now(UTC),
                    rows_in=transform_res["rows_in"],
                    rows_out=transform_res["rows_out"],
                    rows_rejected=transform_res["rows_rejected"],
                )

            pipeline_metrics["silver_rows"] = total_silver_out
            pipeline_metrics["quarantined_rows"] = total_quarantine

            # ------------------------------------------------------------------
            # Stage 5: Silver Quality Gate (HARD BARRIER)
            # ------------------------------------------------------------------
            logger.info("--- Stage 5: Silver Quality Gate Verification ---")
            QualityEngine.validate_silver_layer(run_id=self.run_id)

            # ------------------------------------------------------------------
            # Stage 6: Gold Dimensional Modeling
            # ------------------------------------------------------------------
            logger.info("--- Stage 6: Building Gold Facts, Dimensions & Analytical Marts ---")
            t_gold_start = datetime.now(UTC)
            gold_counts = GoldDimensionalBuilder.build_gold_layer(batch_id=self.run_id)
            pipeline_metrics["gold_rows"] = gold_counts

            OperationalMetadata.record_task_run(
                run_id=self.run_id,
                task_name="gold_dimensional_modeling",
                status="SUCCESS",
                start_time=t_gold_start,
                end_time=datetime.now(UTC),
                rows_in=total_silver_out,
                rows_out=sum(gold_counts.values()),
            )

            # ------------------------------------------------------------------
            # Stage 7: Gold Quality Gate & Reconciliation (HARD BARRIER)
            # ------------------------------------------------------------------
            logger.info("--- Stage 7: Gold Quality & Financial Reconciliation Gate ---")
            QualityEngine.validate_gold_layer(run_id=self.run_id)

            # ------------------------------------------------------------------
            # Stage 8: Publish to Serving Database & Advance Watermarks
            # ------------------------------------------------------------------
            logger.info("--- Stage 8: Syncing to PostgreSQL Serving Layer & Checkpointing ---")
            t_pub_start = datetime.now(UTC)
            GoldDimensionalBuilder.sync_to_serving_database()

            # Advance watermarks for all processed entities
            now_iso = datetime.now(UTC).isoformat()
            for entity in self.entities:
                WatermarkManager.update_watermark(
                    entity_name=entity,
                    watermark_value=now_iso,
                    batch_id=self.run_id,
                )

            OperationalMetadata.record_task_run(
                run_id=self.run_id,
                task_name="analytics_serving_publication",
                status="SUCCESS",
                start_time=t_pub_start,
                end_time=datetime.now(UTC),
                rows_in=sum(gold_counts.values()),
                rows_out=sum(gold_counts.values()),
            )

            # ------------------------------------------------------------------
            # Pipeline Completion
            # ------------------------------------------------------------------
            OperationalMetadata.end_pipeline_run(run_id=self.run_id, status="SUCCESS")
            pipeline_metrics["status"] = "SUCCESS"
            logger.info("=================================================================")
            logger.info("LAKEHOUSE PIPELINE RUN %s COMPLETED SUCCESSFULLY!", self.run_id)
            logger.info("Metrics: %s", pipeline_metrics)
            logger.info("=================================================================")

            return pipeline_metrics

        except Exception as exc:
            logger.error("Pipeline run %s FAILED: %s", self.run_id, exc)
            OperationalMetadata.end_pipeline_run(
                run_id=self.run_id,
                status="FAILED",
                error_message=str(exc),
            )
            pipeline_metrics["status"] = "FAILED"
            pipeline_metrics["error"] = str(exc)
            raise exc
