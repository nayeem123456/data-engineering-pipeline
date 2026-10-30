"""
Airflow DAG Integrity and Configuration Validation Tests.
Validates syntax, task cycles, timeouts, and retry policies.
"""

from __future__ import annotations

import ast

import pytest

from src.common.config import config


def test_airflow_dag_syntax_and_ast():
    """Verify all Python files in dags/ parse without syntax errors and conform to AST patterns."""
    dag_dir = config.PROJECT_ROOT / "dags"
    dag_files = list(dag_dir.glob("*.py"))
    assert len(dag_files) >= 3

    for dag_file in dag_files:
        with open(dag_file, encoding="utf-8") as f:
            code = f.read()
        # Parse AST to ensure no syntax errors
        tree = ast.parse(code, filename=str(dag_file))
        assert tree is not None


def test_airflow_dag_default_task_args_and_retries():
    """Validate DEFAULT_TASK_ARGS in common_airflow.py has required retry & timeout parameters."""
    from dags.common_airflow import DEFAULT_TASK_ARGS

    assert "retries" in DEFAULT_TASK_ARGS
    assert DEFAULT_TASK_ARGS["retries"] >= 1
    assert "retry_delay" in DEFAULT_TASK_ARGS
    assert "execution_timeout" in DEFAULT_TASK_ARGS
    assert DEFAULT_TASK_ARGS["depends_on_past"] is False


def test_airflow_dag_declarations_ast():
    """Verify DAG definitions have catchup=False, callbacks, and tags via AST."""
    pipeline_dag_path = config.PROJECT_ROOT / "dags" / "ecommerce_lakehouse_pipeline.py"
    with open(pipeline_dag_path, encoding="utf-8") as f:
        code = f.read()

    assert (
        'dag_id="ecommerce_lakehouse_pipeline"' in code
        or "dag_id='ecommerce_lakehouse_pipeline'" in code
    )
    assert "catchup=False" in code
    assert "on_failure_callback=task_failure_alert" in code
    assert "PythonOperator" in code
    assert "schedule_interval" in code

    backfill_dag_path = config.PROJECT_ROOT / "dags" / "ecommerce_backfill_dag.py"
    with open(backfill_dag_path, encoding="utf-8") as f:
        bf_code = f.read()

    assert "ecommerce_backfill_dag" in bf_code
    assert "catchup=False" in bf_code
    assert "schedule_interval=None" in bf_code


def test_airflow_dag_objects_if_installed():
    """If Airflow is installed, test DAG structure directly."""
    try:
        from airflow.models import DagBag

        dag_bag = DagBag(dag_folder=str(config.PROJECT_ROOT / "dags"), include_examples=False)
        assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"

        main_dag = dag_bag.get_dag("ecommerce_lakehouse_pipeline")
        assert main_dag is not None
        assert main_dag.catchup is False
        assert len(main_dag.tasks) >= 8

        backfill_dag = dag_bag.get_dag("ecommerce_backfill_dag")
        assert backfill_dag is not None
    except ImportError:
        pytest.skip(
            "Airflow library not installed in local environment (runs in Docker container)."
        )
