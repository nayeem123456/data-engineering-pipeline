"""
Configuration module for the Production Data Lakehouse Platform.
Manages environment variables, storage paths, database credentials, and execution parameters.
"""

from __future__ import annotations

import os
from pathlib import Path


class PlatformConfig:
    """Strongly-typed platform configuration loaded from environment or defaults."""

    # Project Root
    PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent.parent

    # Runtime Environment
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "development")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    DEBUG: bool = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")

    # Data Lakehouse Base Paths
    DATA_DIR: Path = PROJECT_ROOT / os.getenv("DATA_DIR", "data")
    LAKEHOUSE_BASE: Path = PROJECT_ROOT / os.getenv("LAKEHOUSE_BASE_PATH", "data/lakehouse")
    BRONZE_DIR: Path = PROJECT_ROOT / os.getenv("BRONZE_BASE_PATH", "data/lakehouse/bronze")
    SILVER_DIR: Path = PROJECT_ROOT / os.getenv("SILVER_BASE_PATH", "data/lakehouse/silver")
    GOLD_DIR: Path = PROJECT_ROOT / os.getenv("GOLD_BASE_PATH", "data/lakehouse/gold")
    QUARANTINE_DIR: Path = PROJECT_ROOT / os.getenv(
        "QUARANTINE_BASE_PATH", "data/lakehouse/quarantine"
    )
    LOGS_DIR: Path = PROJECT_ROOT / os.getenv("LOGS_DIR", "logs")

    # Database Configuration
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "lakehouse_admin")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "lakehouse_secure_pass_2026")
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "lakehouse_dw")
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))

    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}",
    )

    USE_SQLITE_METADATA_FALLBACK: bool = os.getenv(
        "USE_SQLITE_METADATA_FALLBACK", "true"
    ).lower() in ("true", "1", "yes")
    SQLITE_DB_PATH: Path = PROJECT_ROOT / os.getenv("SQLITE_DB_PATH", "data/metadata_store.db")

    # Spark Config
    SPARK_MASTER: str = os.getenv("SPARK_MASTER", "local[*]")
    SPARK_DRIVER_MEMORY: str = os.getenv("SPARK_DRIVER_MEMORY", "2g")
    SPARK_EXECUTOR_MEMORY: str = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")

    # Ingestion & Quality Parameters
    DEFAULT_BATCH_SIZE: int = int(os.getenv("DEFAULT_BATCH_SIZE", "1000"))
    LATE_ARRIVING_GRACE_PERIOD_HOURS: int = int(os.getenv("LATE_ARRIVING_GRACE_PERIOD_HOURS", "24"))
    DATA_QUALITY_MAX_ERROR_THRESHOLD: int = int(os.getenv("DATA_QUALITY_MAX_ERROR_THRESHOLD", "0"))
    STREAMLIT_SERVER_PORT: int = int(os.getenv("STREAMLIT_SERVER_PORT", "8501"))

    @classmethod
    def ensure_directories(cls) -> None:
        """Create lakehouse storage folders if they do not exist."""
        for path in [
            cls.DATA_DIR,
            cls.LAKEHOUSE_BASE,
            cls.BRONZE_DIR,
            cls.SILVER_DIR,
            cls.GOLD_DIR,
            cls.QUARANTINE_DIR,
            cls.LOGS_DIR,
        ]:
            path.mkdir(parents=True, exist_ok=True)


config = PlatformConfig()
