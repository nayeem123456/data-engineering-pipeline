"""
Production Airflow DAG: E-Commerce Lakehouse Medallion Pipeline.
Orchestrates end-to-end ingestion, Bronze storage, Silver cleansing, quality gates,
Gold dimensional modeling, financial reconciliation, and PostgreSQL serving publication.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

# Add project root to sys.path so Airflow worker can import src
AIRFLOW_APP_DIR = Path("/app")
if AIRFLOW_APP_DIR.exists():
    sys.path.insert(0, str(AIRFLOW_APP_DIR))
else:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from dags.common_airflow import DEFAULT_TASK_ARGS, task_failure_alert


def check_environment_and_db(**kwargs):
    """Verify Lakehouse directories and metadata database connectivity."""
    from src.common.config import config
    from src.monitoring.operational_metadata import OperationalMetadata

    config.ensure_directories()
    OperationalMetadata.initialize_tables()
    return "Environment and database initialized successfully."


def ingest_sources_to_bronze(**kwargs):
    """Generate or collect raw source batch and write to Bronze layer."""
    from src.common.config import config
    from src.ingestion.generator import ECommerceDataGenerator
    from src.ingestion.raw_ingester import RawIngester

    run_id = kwargs.get("run_id", f"af_run_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}")
    gen = ECommerceDataGenerator()
    products = gen.generate_products(count=30)
    customers = gen.generate_customers(count=60)
    batch_data = gen.generate_orders_and_related(
        customers=customers, products=products, order_count=150
    )
    batch_data["products"] = products
    batch_data["customers"] = customers

    staging_dir = config.DATA_DIR / "staging" / run_id
    staging_files = gen.write_dataset_to_staging(
        batch_data, staging_dir, run_id, format_type="json"
    )

    res = RawIngester.ingest_batch(
        files=staging_files,
        source_name="ecommerce_platform",
        batch_id=run_id,
        batch_type="INCREMENTAL",
    )
    return res


def validate_bronze_quality(**kwargs):
    """Verify Bronze Parquet file readability and audit columns."""
    from src.common.config import config
    from src.validation.quality_engine import QualityEngine

    for ent in ["orders", "order_items", "customers", "products", "payments"]:
        ent_dir = config.BRONZE_DIR / ent
        if ent_dir.exists():
            files = list(ent_dir.glob("**/*.parquet"))
            if files:
                QualityEngine.validate_bronze_layer(entity=ent, file_path=files[-1])
    return "Bronze quality verified."


def cleanse_and_conform_silver(**kwargs):
    """Execute Silver cleansing, deduplication, and quarantine isolation."""
    from src.transformations.engine_adapter import SilverTransformationEngine

    run_id = kwargs.get("run_id", f"af_run_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}")
    entities = ["orders", "order_items", "customers", "products", "payments"]
    stats = {}
    for ent in entities:
        res = SilverTransformationEngine.transform_entity(entity=ent, batch_id=run_id)
        stats[ent] = res
    return stats


def silver_quality_gate(**kwargs):
    """Hard Quality Gate for Silver tier. Halts on critical check failure."""
    from src.validation.quality_engine import QualityEngine

    run_id = kwargs.get("run_id")
    results = QualityEngine.validate_silver_layer(run_id=run_id)
    return f"Passed {len(results)} Silver quality assertions."


def build_gold_dimensional_models(**kwargs):
    """Transform conformed Silver into Kimball star schema facts, dimensions & marts."""
    from src.transformations.gold_builder import GoldDimensionalBuilder

    run_id = kwargs.get("run_id", f"af_run_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}")
    counts = GoldDimensionalBuilder.build_gold_layer(batch_id=run_id)
    return counts


def gold_quality_and_reconciliation_gate(**kwargs):
    """Hard Quality Gate for Gold tier and financial reconciliation."""
    from src.validation.quality_engine import QualityEngine

    run_id = kwargs.get("run_id")
    results = QualityEngine.validate_gold_layer(run_id=run_id)
    return f"Passed {len(results)} Gold quality assertions."


def publish_to_serving_layer(**kwargs):
    """Publish Gold tables to PostgreSQL serving schema and advance watermarks."""
    from src.storage.watermark_manager import WatermarkManager
    from src.transformations.gold_builder import GoldDimensionalBuilder

    run_id = kwargs.get("run_id", f"af_run_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}")
    GoldDimensionalBuilder.sync_to_serving_database()

    now_iso = datetime.now(UTC).isoformat()
    for ent in ["orders", "order_items", "customers", "products", "payments"]:
        WatermarkManager.update_watermark(entity_name=ent, watermark_value=now_iso, batch_id=run_id)
    return "Serving layer publication and checkpointing complete."


def operational_housekeeping(**kwargs):
    """Prune temporary files and log pipeline run summary."""
    import shutil

    from src.common.config import config

    staging_dir = config.DATA_DIR / "staging"
    if staging_dir.exists():
        # Keep recent, remove older temp files
        for sub in staging_dir.iterdir():
            if sub.is_dir() and sub.name.startswith("temp_"):
                shutil.rmtree(sub, ignore_errors=True)
    return "Housekeeping complete."


with DAG(
    dag_id="ecommerce_lakehouse_pipeline",
    default_args=DEFAULT_TASK_ARGS,
    description="End-to-end Medallion Data Lakehouse & ETL Platform Pipeline",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "medallion", "production", "ecommerce"],
    on_failure_callback=task_failure_alert,
) as dag:
    start_task = EmptyOperator(task_id="pipeline_start")

    init_env = PythonOperator(
        task_id="check_environment_and_db",
        python_callable=check_environment_and_db,
    )

    ingest_bronze = PythonOperator(
        task_id="ingest_sources_to_bronze",
        python_callable=ingest_sources_to_bronze,
    )

    check_bronze_dq = PythonOperator(
        task_id="validate_bronze_quality",
        python_callable=validate_bronze_quality,
    )

    clean_silver = PythonOperator(
        task_id="cleanse_and_conform_silver",
        python_callable=cleanse_and_conform_silver,
    )

    silver_dq_gate = PythonOperator(
        task_id="silver_quality_gate",
        python_callable=silver_quality_gate,
    )

    build_gold = PythonOperator(
        task_id="build_gold_dimensional_models",
        python_callable=build_gold_dimensional_models,
    )

    gold_dq_gate = PythonOperator(
        task_id="gold_quality_and_reconciliation_gate",
        python_callable=gold_quality_and_reconciliation_gate,
    )

    publish_serving = PythonOperator(
        task_id="publish_to_serving_layer",
        python_callable=publish_to_serving_layer,
    )

    housekeeping = PythonOperator(
        task_id="operational_housekeeping",
        python_callable=operational_housekeeping,
    )

    end_task = EmptyOperator(task_id="pipeline_end")

    # Strict Task Dependency Flow
    (
        start_task
        >> init_env
        >> ingest_bronze
        >> check_bronze_dq
        >> clean_silver
        >> silver_dq_gate
        >> build_gold
        >> gold_dq_gate
        >> publish_serving
        >> housekeeping
        >> end_task
    )
