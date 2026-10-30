"""
Source File Manifest and File-Level Deduplication.
Computes cryptographic digests to detect and prevent reprocessing of duplicate files.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import text

from src.common.db import db_manager
from src.common.logger import get_logger
from src.storage.lakehouse_fs import LakehouseFS

logger = get_logger("platform.ingestion.manifest")


class ManifestManager:
    """Tracks source files and enforces file-level deduplication."""

    @classmethod
    def check_file(cls, file_path: Path) -> tuple[bool, str]:
        """
        Check if file has already been processed based on SHA-256 hash.
        Returns: (is_duplicate, sha256_checksum)
        """
        checksum = LakehouseFS.calculate_file_checksum(file_path)
        table = (
            "metadata_source_file_manifests"
            if db_manager.is_sqlite()
            else "metadata.source_file_manifests"
        )
        query = text(
            f"SELECT file_id, batch_id, status FROM {table} WHERE file_checksum_sha256 = :chk"
        )

        try:
            with db_manager.get_session() as session:
                res = session.execute(query, {"chk": checksum}).fetchone()
                if res and res[2] == "PROCESSED":
                    logger.warning(
                        "Duplicate file detected: %s matches checksum %s from batch %s. Skipping re-processing.",
                        file_path.name,
                        checksum[:12],
                        res[1],
                    )
                    return True, checksum
        except Exception as exc:
            logger.warning("Could not check manifest for %s: %s", file_path.name, exc)

        return False, checksum

    @classmethod
    def register_file(
        cls,
        batch_id: str,
        file_path: Path,
        checksum: str,
        record_count: int,
        status: str = "PROCESSED",
    ) -> None:
        """Register file processing status in the manifest table."""
        table = (
            "metadata_source_file_manifests"
            if db_manager.is_sqlite()
            else "metadata.source_file_manifests"
        )
        file_id = f"file_{uuid.uuid4().hex[:12]}"
        file_size = file_path.stat().st_size
        now = datetime.now(UTC)

        if db_manager.is_sqlite():
            query = text(f"""
                INSERT INTO {table} (file_id, batch_id, file_name, file_path, file_checksum_sha256, file_size_bytes, record_count, status, ingested_at)
                VALUES (:fid, :bid, :fname, :fpath, :chk, :fsize, :rcount, :status, :now)
                ON CONFLICT(file_checksum_sha256) DO UPDATE SET
                    status = excluded.status,
                    batch_id = excluded.batch_id
            """)
        else:
            query = text(f"""
                INSERT INTO {table} (file_id, batch_id, file_name, file_path, file_checksum_sha256, file_size_bytes, record_count, status, ingested_at)
                VALUES (:fid, :bid, :fname, :fpath, :chk, :fsize, :rcount, :status, :now)
                ON CONFLICT(file_checksum_sha256) DO UPDATE SET
                    status = EXCLUDED.status,
                    batch_id = EXCLUDED.batch_id
            """)

        with db_manager.get_session() as session:
            session.execute(
                query,
                {
                    "fid": file_id,
                    "bid": batch_id,
                    "fname": file_path.name,
                    "fpath": str(file_path),
                    "chk": checksum,
                    "fsize": file_size,
                    "rcount": record_count,
                    "status": status,
                    "now": now,
                },
            )
        logger.info(
            "Registered file %s in manifest (checksum: %s, records: %d)",
            file_path.name,
            checksum[:12],
            record_count,
        )
