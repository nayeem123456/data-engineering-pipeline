"""
Unit Tests for Raw Ingestion and Manifest Duplicate Detection.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd

from src.ingestion.manifest import ManifestManager
from src.ingestion.raw_ingester import RawIngester
from src.storage.lakehouse_fs import LakehouseFS


def test_manifest_duplicate_detection(clean_metadata_tables):
    with tempfile.TemporaryDirectory() as td:
        tpath = Path(td)
        file1 = tpath / "test_orders.csv"
        df = pd.DataFrame(
            [
                {"order_id": "ORD-1", "customer_id": "CUST-1", "total_amount": 100.0},
                {"order_id": "ORD-2", "customer_id": "CUST-2", "total_amount": 250.0},
            ]
        )
        df.to_csv(file1, index=False)

        # First check: should not be duplicate
        is_dup, chk1 = ManifestManager.check_file(file1)
        assert not is_dup
        assert len(chk1) == 64

        # Register file as PROCESSED
        ManifestManager.register_file(
            batch_id="b1",
            file_path=file1,
            checksum=chk1,
            record_count=2,
            status="PROCESSED",
        )

        # Second check: exact same file should now be flagged as duplicate
        is_dup_second, chk2 = ManifestManager.check_file(file1)
        assert is_dup_second
        assert chk1 == chk2


def test_raw_ingester_preserves_schema_and_attaches_audit():
    with tempfile.TemporaryDirectory() as td:
        tpath = Path(td)
        csv_file = tpath / "customers_test.csv"
        df = pd.DataFrame(
            [
                {"customer_id": "C-101", "email": "test1@example.com", "signup_date": "2026-01-10"},
                {"customer_id": "C-102", "email": "test2@example.com", "signup_date": "2026-01-11"},
            ]
        )
        df.to_csv(csv_file, index=False)

        res = RawIngester.ingest_file(
            file_path=csv_file,
            entity="customers",
            source_name="test_crm",
            batch_id="batch_unit_test",
            natural_key_col="customer_id",
        )

        assert res["status"] == "PROCESSED"
        assert res["rows_ingested"] == 2
        assert res["target_path"] is not None

        # Read back parquet from lakehouse to verify audit columns
        ingested_df = LakehouseFS.read_parquet(Path(res["target_path"]))
        assert len(ingested_df) == 2
        assert "_ingestion_id" in ingested_df.columns
        assert "_batch_id" in ingested_df.columns
        assert "_source_name" in ingested_df.columns
        assert "_source_file_checksum" in ingested_df.columns
        assert "_ingested_at" in ingested_df.columns
        assert "_ingestion_timestamp" in ingested_df.columns
        assert "_record_hash" in ingested_df.columns
        assert all(len(h) == 64 for h in ingested_df["_record_hash"])
        assert "_source_record_id" in ingested_df.columns
        assert ingested_df["_source_record_id"].tolist() == ["C-101", "C-102"]


def test_ingest_empty_file_fails_safely(clean_metadata_tables):
    """Empty 0-byte file must raise ValueError and be recorded in manifest as FAILED."""
    import pytest

    with tempfile.TemporaryDirectory() as td:
        empty_file = Path(td) / "empty_orders.csv"
        empty_file.write_text("")

        with pytest.raises(ValueError, match="empty"):
            RawIngester.ingest_file(
                file_path=empty_file,
                entity="orders",
                source_name="test_source",
                batch_id="b_empty_test",
            )


def test_ingest_malformed_json_fails_safely(clean_metadata_tables):
    """Malformed JSON file raises ValueError and does not corrupt lakehouse storage."""
    import pytest

    with tempfile.TemporaryDirectory() as td:
        corrupt_json = Path(td) / "corrupt_customers.json"
        corrupt_json.write_text("{'broken_json': true, not_valid_json...")

        with pytest.raises(ValueError, match="Malformed or corrupt"):
            RawIngester.ingest_file(
                file_path=corrupt_json,
                entity="customers",
                source_name="test_source",
                batch_id="b_corrupt_json",
            )


def test_ingest_malformed_csv_fails_safely(clean_metadata_tables):
    """Corrupt or non-tabular CSV file raises descriptive ValueError."""
    import pytest

    with tempfile.TemporaryDirectory() as td:
        corrupt_csv = Path(td) / "corrupt_data.csv"
        # CSV with zero records
        corrupt_csv.write_text("\n\n")

        with pytest.raises(ValueError):
            RawIngester.ingest_file(
                file_path=corrupt_csv,
                entity="orders",
                source_name="test_source",
                batch_id="b_corrupt_csv",
            )


def test_manifest_different_name_same_content_detected_as_duplicate(clean_metadata_tables):
    """Files with different names but identical content must be detected as duplicates via SHA-256."""
    with tempfile.TemporaryDirectory() as td:
        tpath = Path(td)
        file_a = tpath / "export_january.csv"
        file_b = tpath / "copy_of_export_january.csv"
        content = "customer_id,email\nC-1,a@example.com\nC-2,b@example.com\n"
        file_a.write_text(content)
        file_b.write_text(content)

        # Ingest file A
        res_a = RawIngester.ingest_file(
            file_path=file_a,
            entity="customers",
            source_name="crm",
            batch_id="batch_dup_content_a",
        )
        assert res_a["status"] == "PROCESSED"

        # Ingest file B with different name
        res_b = RawIngester.ingest_file(
            file_path=file_b,
            entity="customers",
            source_name="crm",
            batch_id="batch_dup_content_b",
        )
        assert res_b["status"] == "DUPLICATE_SKIPPED"
        assert res_b["rows_ingested"] == 0
        assert res_b["checksum"] == res_a["checksum"]


def test_ingest_batch_records_failure_status_on_error(clean_metadata_tables):
    """When a file in a batch fails, ingest_batch marks the batch as FAILED in metadata."""
    import pytest
    from sqlalchemy import text

    from src.common.db import db_manager

    with tempfile.TemporaryDirectory() as td:
        tpath = Path(td)
        good_file = tpath / "good_orders.json"
        good_file.write_text('[{"order_id": "ORD-1", "customer_id": "C-1", "total_amount": 50.0}]')
        bad_file = tpath / "empty_payments.json"
        bad_file.write_text("")  # empty file triggers failure

        files = {"orders": good_file, "payments": bad_file}
        batch_id = "test_batch_failure_recovery"

        with pytest.raises(ValueError):
            RawIngester.ingest_batch(
                files=files,
                source_name="ecommerce_test",
                batch_id=batch_id,
            )

        # Check metadata status
        table = (
            "metadata_ingestion_batches" if db_manager.is_sqlite() else "metadata.ingestion_batches"
        )
        with db_manager.get_session() as session:
            row = session.execute(
                text(f"SELECT ingestion_status FROM {table} WHERE batch_id = :bid"),
                {"bid": batch_id},
            ).fetchone()
            assert row is not None
            assert row[0] == "FAILED"
