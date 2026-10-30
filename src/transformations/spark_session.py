"""
PySpark Session Factory and Local Execution Tuning.
Configures distributed execution settings, shuffle partitions, and memory optimization.
"""

from __future__ import annotations

from src.common.config import config
from src.common.logger import get_logger

logger = get_logger("platform.transformations.spark_session")


class SparkSessionFactory:
    """Creates and configures high-performance Spark sessions for Lakehouse jobs."""

    _spark_session = None

    @classmethod
    def get_session(cls, app_name: str = "LakehouseSilverETL"):
        """Get or create singleton SparkSession with production Lakehouse settings."""
        if cls._spark_session is not None:
            return cls._spark_session

        try:
            from pyspark.sql import SparkSession

            builder = (
                SparkSession.builder.appName(app_name)
                .master(config.SPARK_MASTER)
                .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY)
                .config("spark.executor.memory", config.SPARK_EXECUTOR_MEMORY)
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .config("spark.sql.parquet.compression.codec", "snappy")
                .config("spark.ui.enabled", "false")  # Disable web UI for local batch speed
            )

            cls._spark_session = builder.getOrCreate()
            cls._spark_session.sparkContext.setLogLevel("WARN")
            logger.info(
                "Initialized PySpark Session: %s (master=%s)", app_name, config.SPARK_MASTER
            )
            return cls._spark_session

        except Exception as exc:
            logger.warning("Could not initialize SparkSession: %s", exc)
            return None

    @classmethod
    def stop_session(cls) -> None:
        """Stop running SparkSession."""
        if cls._spark_session is not None:
            cls._spark_session.stop()
            cls._spark_session = None
            logger.info("Stopped PySpark Session.")
