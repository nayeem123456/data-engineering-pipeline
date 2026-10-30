"""
Common Airflow Utilities, Callbacks, and Configuration.
Provides standardized alerting, retry definitions, and failure hooks.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

logger = logging.getLogger("airflow.task")

DEFAULT_TASK_ARGS = {
    "owner": "data_platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),
}


def task_failure_alert(context: dict[str, Any]) -> None:
    """Callback triggered on task failure for structured alerting."""
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "unknown"
    dag_id = task_instance.dag_id if task_instance else "unknown"
    execution_date = context.get("execution_date")
    exception = context.get("exception")

    logger.error(
        "CRITICAL AIRFLOW TASK FAILURE: DAG=%s, Task=%s, ExecDate=%s, Exception=%s",
        dag_id,
        task_id,
        execution_date,
        exception,
    )


def task_success_notification(context: dict[str, Any]) -> None:
    """Callback on critical milestone completion."""
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "unknown"
    dag_id = task_instance.dag_id if task_instance else "unknown"

    logger.info("AIRFLOW TASK COMPLETED: DAG=%s, Task=%s", dag_id, task_id)
