"""
Unit Tests for Schema Evolution and Contract Validation.
"""

from __future__ import annotations

import pandas as pd

from src.transformations.schema_evolution import SchemaEvolutionManager


def test_schema_evolution_compatible_new_column(clean_metadata_tables):
    # Customer data with an extra optional column: loyalty_points
    df = pd.DataFrame(
        [
            {
                "customer_id": "CUST-999",
                "email": "loyalty@example.com",
                "signup_date": "2026-01-01",
                "loyalty_points": 500,  # new optional column
            }
        ]
    )

    is_compat, changes = SchemaEvolutionManager.evaluate_schema(
        entity="customers",
        df=df,
        layer="SILVER",
    )

    assert is_compat is True
    assert len(changes) == 1
    assert changes[0]["change_type"] == "NEW_OPTIONAL_COLUMN"
    assert changes[0]["column"] == "loyalty_points"


def test_schema_evolution_breaking_missing_required_column(clean_metadata_tables):
    # Customer data missing required 'email'
    df = pd.DataFrame(
        [
            {
                "customer_id": "CUST-999",
                "first_name": "NoEmailPerson",
            }
        ]
    )

    is_compat, changes = SchemaEvolutionManager.evaluate_schema(
        entity="customers",
        df=df,
        layer="SILVER",
    )

    assert is_compat is False
    assert any(c["change_type"] == "MISSING_REQUIRED_COLUMN" for c in changes)


def test_silver_transformation_halts_on_breaking_schema_violation(clean_metadata_tables):
    """SilverTransformationEngine must raise ValueError and stop when breaking change occurs."""
    import pytest

    from src.common.config import config
    from src.storage.lakehouse_fs import LakehouseFS
    from src.transformations.engine_adapter import SilverTransformationEngine

    # Missing email column in customers
    broken_bronze = pd.DataFrame(
        [
            {
                "customer_id": "C-BREAKING",
                "first_name": "MissingEmail",
                "_ingested_at": "2026-01-01T00:00:00Z",
                "_batch_id": "b_break",
            }
        ]
    )
    p = config.BRONZE_DIR / "customers" / "breaking_schema.parquet"
    LakehouseFS.write_parquet(broken_bronze, p)

    with pytest.raises(ValueError, match="Breaking schema violation"):
        SilverTransformationEngine.transform_entity(
            entity="customers",
            batch_id="b_break",
            bronze_files=[p],
        )


def test_silver_transformation_preserves_new_optional_columns(clean_metadata_tables):
    """SilverTransformationEngine accommodates and preserves new optional columns in Silver."""
    from src.common.config import config
    from src.storage.lakehouse_fs import LakehouseFS
    from src.transformations.engine_adapter import SilverTransformationEngine

    valid_bronze = pd.DataFrame(
        [
            {
                "customer_id": "C-LOYALTY",
                "first_name": "Alice",
                "last_name": "Smith",
                "email": "alice.loyalty@example.com",
                "signup_date": "2026-01-01",
                "loyalty_tier": "PLATINUM",  # new optional column
                "_ingested_at": "2026-01-01T00:00:00Z",
                "_batch_id": "b_optional",
            }
        ]
    )
    p = config.BRONZE_DIR / "customers" / "optional_schema.parquet"
    LakehouseFS.write_parquet(valid_bronze, p)

    res = SilverTransformationEngine.transform_entity(
        entity="customers",
        batch_id="b_optional",
        bronze_files=[p],
    )
    assert res["rows_out"] >= 1

    silver_df = LakehouseFS.read_parquet(config.SILVER_DIR / "customers" / "data.parquet")
    assert "loyalty_tier" in silver_df.columns
    assert (
        silver_df.loc[silver_df["customer_id"] == "C-LOYALTY", "loyalty_tier"].iloc[0] == "PLATINUM"
    )
