"""
Raw Data Ingestion Engine (Bronze Layer).
Preserves raw source payloads, calculates SHA-256 manifests, prevents duplicate file loading,
attaches audit lineage columns, and partitions Parquet files by ingestion_date.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text

from src.common.db import db_manager
from src.common.logger import get_logger
from src.ingestion.manifest import ManifestManager
from src.storage.lakehouse_fs import LakehouseFS

logger = get_logger("platform.ingestion.raw_ingester")


class RawIngester:
    """Ingests raw CSV/JSON files into partitioned Bronze Parquet lakehouse storage."""

    @classmethod
    def ingest_file(
        cls,
        file_path: Path,
        entity: str,
        source_name: str,
        batch_id: str,
        natural_key_col: str | None = None,
    ) -> dict[str, Any]:
        """
        Ingest a single CSV or JSON file into the Bronze layer.
        Performs duplicate file detection, attaches audit metadata, and writes Parquet.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"Source file not found: {file_path}")

        # Check for file-level duplicates
        is_duplicate, checksum = ManifestManager.check_file(file_path)
        if is_duplicate:
            return {
                "entity": entity,
                "file_name": file_path.name,
                "status": "DUPLICATE_SKIPPED",
                "rows_ingested": 0,
                "checksum": checksum,
                "target_path": None,
            }

        # Check for empty file
        if file_path.stat().st_size == 0:
            ManifestManager.register_file(
                batch_id=batch_id,
                file_path=file_path,
                checksum=checksum,
                record_count=0,
                status="FAILED",
            )
            raise ValueError(f"Source file '{file_path.name}' is empty (0 bytes).")

        # Read raw data into DataFrame with validation
        try:
            if file_path.suffix.lower() == ".csv":
                df = pd.read_csv(file_path, dtype=str)  # preserve raw representations
            elif file_path.suffix.lower() == ".json":
                df = pd.read_json(file_path, dtype=False)
            else:
                raise ValueError(f"Unsupported file format: {file_path.suffix}")
        except Exception as read_err:
            ManifestManager.register_file(
                batch_id=batch_id,
                file_path=file_path,
                checksum=checksum,
                record_count=0,
                status="FAILED",
            )
            if isinstance(read_err, ValueError) and "Unsupported file format" in str(read_err):
                raise read_err
            raise ValueError(
                f"Malformed or corrupt file '{file_path.name}': {read_err}"
            ) from read_err

        if df.empty:
            ManifestManager.register_file(
                batch_id=batch_id,
                file_path=file_path,
                checksum=checksum,
                record_count=0,
                status="FAILED",
            )
            raise ValueError(f"Source file '{file_path.name}' contains no data records.")

        row_count = len(df)
        ingest_time = datetime.now(UTC)
        ingestion_date_str = ingest_time.strftime("%Y-%m-%d")

        # Compute SHA-256 hash for each raw record payload for data integrity auditing
        import hashlib
        import json

        raw_records = df.to_dict(orient="records")
        record_hashes = [
            hashlib.sha256(json.dumps(r, sort_keys=True, default=str).encode("utf-8")).hexdigest()
            for r in raw_records
        ]

        # Attach Bronze audit columns
        df["_ingestion_id"] = [str(uuid.uuid4()) for _ in range(row_count)]
        df["_batch_id"] = batch_id
        df["_source_name"] = source_name
        df["_source_file"] = file_path.name
        df["_source_file_checksum"] = checksum
        df["_ingestion_timestamp"] = ingest_time.isoformat()
        df["_ingested_at"] = ingest_time.isoformat()
        df["_record_hash"] = record_hashes

        if natural_key_col and natural_key_col in df.columns:
            df["_source_record_id"] = df[natural_key_col].astype(str)
        else:
            df["_source_record_id"] = [f"rec_{i}" for i in range(row_count)]

        # Write to Bronze Parquet
        target_path = LakehouseFS.write_bronze_batch(
            df=df,
            entity=entity,
            batch_id=f"{batch_id}_{file_path.stem}",
            ingestion_date=ingestion_date_str,
        )

        # Register in manifest
        ManifestManager.register_file(
            batch_id=batch_id,
            file_path=file_path,
            checksum=checksum,
            record_count=row_count,
            status="PROCESSED",
        )

        logger.info(
            "Ingested %d rows of '%s' from %s into Bronze at %s",
            row_count,
            entity,
            file_path.name,
            target_path,
        )

        return {
            "entity": entity,
            "file_name": file_path.name,
            "status": "PROCESSED",
            "rows_ingested": row_count,
            "checksum": checksum,
            "target_path": str(target_path),
        }

    @classmethod
    def ingest_batch(
        cls,
        files: dict[str, Path],
        source_name: str,
        batch_id: str,
        batch_type: str = "INCREMENTAL",
    ) -> dict[str, Any]:
        """Ingest multiple entity files for a given batch."""
        started_at = datetime.now(UTC)
        total_rows = 0
        results = []

        # Record batch initiation in metadata
        table = (
            "metadata_ingestion_batches" if db_manager.is_sqlite() else "metadata.ingestion_batches"
        )
        init_query = text(f"""
            INSERT INTO {table} (batch_id, source_name, entity, batch_type, record_count, file_count, ingestion_status, started_at)
            VALUES (:bid, :src, 'ALL', :btype, 0, :fcount, 'RUNNING', :start)
        """)
        with db_manager.get_session() as session:
            session.execute(
                init_query,
                {
                    "bid": batch_id,
                    "src": source_name,
                    "btype": batch_type,
                    "fcount": len(files),
                    "start": started_at,
                },
            )

        try:
            for entity, file_path in files.items():
                res = cls.ingest_file(
                    file_path=file_path,
                    entity=entity,
                    source_name=source_name,
                    batch_id=batch_id,
                    natural_key_col=f"{entity[:-1]}_id" if entity.endswith("s") else f"{entity}_id",
                )
                total_rows += res["rows_ingested"]
                results.append(res)

            completed_at = datetime.now(UTC)
            update_query = text(f"""
                UPDATE {table}
                SET record_count = :count, ingestion_status = 'COMPLETED', completed_at = :end
                WHERE batch_id = :bid
            """)
            with db_manager.get_session() as session:
                session.execute(
                    update_query, {"count": total_rows, "end": completed_at, "bid": batch_id}
                )

            return {
                "batch_id": batch_id,
                "total_rows": total_rows,
                "file_count": len(files),
                "results": results,
            }
        except Exception as batch_err:
            failed_at = datetime.now(UTC)
            fail_query = text(f"""
                UPDATE {table}
                SET ingestion_status = 'FAILED', completed_at = :end
                WHERE batch_id = :bid
            """)
            try:
                with db_manager.get_session() as session:
                    session.execute(fail_query, {"end": failed_at, "bid": batch_id})
            except Exception as meta_err:
                logger.warning("Could not update batch failure status in metadata: %s", meta_err)
            raise batch_err
