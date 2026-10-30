"""
Production Airflow DAG: Historical Backfill & Partition Reprocessing.
Parameterized workflow allowing operators to reprocess historical intervals on-demand.
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

AIRFLOW_APP_DIR = Path("/app")
if AIRFLOW_APP_DIR.exists():
    sys.path.insert(0, str(AIRFLOW_APP_DIR))
else:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from dags.common_airflow import DEFAULT_TASK_ARGS, task_failure_alert


def execute_historical_backfill(**kwargs):
    """Execute historical backfill using provided DAG run parameters."""
    from src.orchestration.backfill_runner import BackfillCoordinator

    params = kwargs.get("params", {})
    start_date = params.get("start_date", "2026-01-01")
    end_date = params.get("end_date", "2026-01-07")
    entities = params.get(
        "entities", ["orders", "order_items", "customers", "products", "payments"]
    )

    result = BackfillCoordinator.execute_backfill(
        start_date=start_date,
        end_date=end_date,
        entities=entities,
    )
    return result


with DAG(
    dag_id="ecommerce_backfill_dag",
    default_args=DEFAULT_TASK_ARGS,
    description="Parameterized Historical Backfill and Reprocessing",
    schedule_interval=None,  # Triggered manually on demand
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "backfill", "reprocessing"],
    on_failure_callback=task_failure_alert,
    params={
        "start_date": Param(
            "2026-01-01", type="string", description="Interval start date (YYYY-MM-DD)"
        ),
        "end_date": Param(
            "2026-01-07", type="string", description="Interval end date (YYYY-MM-DD)"
        ),
        "entities": Param(
            ["orders", "order_items", "customers", "products", "payments"],
            type="array",
            description="List of entities to reprocess",
        ),
    },
) as dag:
    start_task = EmptyOperator(task_id="backfill_start")

    run_backfill = PythonOperator(
        task_id="execute_backfill_partitions",
        python_callable=execute_historical_backfill,
    )

    end_task = EmptyOperator(task_id="backfill_end")

    start_task >> run_backfill >> end_task
