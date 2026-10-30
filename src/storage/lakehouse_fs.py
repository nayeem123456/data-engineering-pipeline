"""
Lakehouse File System and Storage Abstraction.
Handles atomic Parquet read/write operations, partitioning, and cryptographic checksums.
"""

from __future__ import annotations

import hashlib
import shutil
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.common.config import config
from src.common.logger import get_logger

logger = get_logger("platform.storage.lakehouse_fs")


class LakehouseFS:
    """Manages Parquet storage, partition layouts, and file integrity checksums."""

    @staticmethod
    def calculate_file_checksum(file_path: Path) -> str:
        """Calculate the SHA-256 hash of a file for duplicate detection."""
        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        return hasher.hexdigest()

    @staticmethod
    def write_parquet(
        df: pd.DataFrame | pa.Table,
        target_path: Path,
        compression: str = "snappy",
    ) -> Path:
        """Write a DataFrame or PyArrow Table to a Parquet file atomically."""
        target_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = target_path.with_suffix(f".tmp_{uuid.uuid4().hex[:8]}.parquet")

        table = pa.Table.from_pandas(df) if isinstance(df, pd.DataFrame) else df

        pq.write_table(table, temp_path, compression=compression)
        shutil.move(temp_path, target_path)
        logger.debug("Successfully wrote Parquet to %s (%d rows)", target_path, table.num_rows)
        return target_path

    @staticmethod
    def read_parquet(source_path: Path) -> pd.DataFrame:
        """Read Parquet file or directory into a pandas DataFrame."""
        if not source_path.exists():
            return pd.DataFrame()

        try:
            table = pq.read_table(source_path)
            return table.to_pandas()
        except Exception as exc:
            logger.error("Failed to read Parquet from %s: %s", source_path, exc)
            raise exc

    @classmethod
    def write_bronze_batch(
        cls,
        df: pd.DataFrame,
        entity: str,
        batch_id: str,
        ingestion_date: str | None = None,
    ) -> Path:
        """
        Write raw bronze partition: data/lakehouse/bronze/{entity}/ingestion_date=YYYY-MM-DD/{batch_id}.parquet
        """
        if ingestion_date is None:
            ingestion_date = datetime.now(UTC).strftime("%Y-%m-%d")

        partition_dir = config.BRONZE_DIR / entity / f"ingestion_date={ingestion_date}"
        target_file = partition_dir / f"{batch_id}.parquet"
        return cls.write_parquet(df, target_file)

    @classmethod
    def write_silver_dataset(
        cls,
        df: pd.DataFrame,
        entity: str,
        mode: str = "overwrite",
    ) -> Path:
        """
        Write cleansed Silver dataset: data/lakehouse/silver/{entity}/data.parquet
        """
        entity_dir = config.SILVER_DIR / entity
        target_file = entity_dir / "data.parquet"

        if mode == "append" and target_file.exists():
            existing_df = cls.read_parquet(target_file)
            df = pd.concat([existing_df, df], ignore_index=True)

        return cls.write_parquet(df, target_file)

    @classmethod
    def write_quarantine_records(
        cls,
        records: list[dict[str, Any]],
        entity: str,
        batch_id: str,
    ) -> Path | None:
        """
        Write rejected records to quarantine layer: data/lakehouse/quarantine/{entity}/quarantine_date=YYYY-MM-DD/
        """
        if not records:
            return None

        today = datetime.now(UTC).strftime("%Y-%m-%d")
        quarantine_dir = config.QUARANTINE_DIR / entity / f"quarantine_date={today}"
        target_file = quarantine_dir / f"quarantine_{batch_id}.parquet"
        df = pd.DataFrame(records)
        return cls.write_parquet(df, target_file)

    @classmethod
    def write_gold_table(
        cls,
        df: pd.DataFrame,
        table_name: str,
    ) -> Path:
        """
        Write analytical gold table: data/lakehouse/gold/{table_name}/data.parquet
        """
        target_dir = config.GOLD_DIR / table_name
        target_file = target_dir / "data.parquet"
        return cls.write_parquet(df, target_file)
