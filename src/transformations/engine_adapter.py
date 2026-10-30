"""
Engine Adapter for Silver Transformations.
Supports PySpark distributed processing and native high-speed DuckDB/PyArrow execution.
Guarantees identical deterministic deduplication rules, schema standardization, and quarantine routing.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from src.common.config import config
from src.common.logger import get_logger
from src.storage.lakehouse_fs import LakehouseFS
from src.transformations.schema_evolution import SchemaEvolutionManager
from src.validation.quarantine import QuarantineManager

logger = get_logger("platform.transformations.engine_adapter")


class SilverTransformationEngine:
    """Unified engine adapter for Lakehouse Silver tier cleansing and deduplication."""

    @classmethod
    def transform_entity(
        cls,
        entity: str,
        batch_id: str,
        bronze_files: list[Path] | None = None,
        use_spark_if_available: bool = False,
    ) -> dict[str, Any]:
        """
        Transform Bronze data for an entity into conformed, deduplicated Silver Parquet.
        """
        entity_bronze_dir = config.BRONZE_DIR / entity
        if not entity_bronze_dir.exists():
            return {"entity": entity, "rows_in": 0, "rows_out": 0, "rows_rejected": 0}

        # Collect bronze parquet files
        if bronze_files:
            target_files = [str(f) for f in bronze_files if f.exists()]
        else:
            target_files = [str(f) for f in entity_bronze_dir.glob("**/*.parquet") if f.is_file()]

        if not target_files:
            return {"entity": entity, "rows_in": 0, "rows_out": 0, "rows_rejected": 0}

        # Try PySpark if requested and available
        if use_spark_if_available:
            try:
                from src.transformations.spark_session import SparkSessionFactory
                from src.transformations.spark_silver import SparkSilverTransformer

                if SparkSessionFactory.get_session() is not None:
                    silver_dir = config.SILVER_DIR / entity
                    return SparkSilverTransformer.process_orders(
                        bronze_path=entity_bronze_dir,
                        silver_path=silver_dir,
                        batch_id=batch_id,
                    )
            except Exception as spark_err:
                logger.warning("Spark execution skipped (%s). Using DuckDB engine.", spark_err)

        # Default high-performance DuckDB / PyArrow execution engine
        con = duckdb.connect(database=":memory:")

        # Read bronze dataset
        bronze_view_sql = f"CREATE VIEW raw_bronze AS SELECT * FROM read_parquet({target_files!r})"
        con.execute(bronze_view_sql)
        rows_in = con.execute("SELECT count(*) FROM raw_bronze").fetchone()[0]

        datetime.now(UTC).isoformat()
        quarantined_count = 0

        cols = set(con.execute("DESCRIBE raw_bronze").df()["column_name"].str.lower().tolist())

        # Required input columns per entity that cannot be derived
        mandatory_cols_by_entity = {
            "orders": ["order_id", "customer_id", "order_status"],
            "customers": ["customer_id", "email"],
            "products": ["product_id", "product_name", "category", "retail_price"],
            "order_items": ["order_id", "product_id", "quantity", "unit_price"],
            "payments": ["payment_id", "order_id", "amount"],
        }
        if entity in mandatory_cols_by_entity:
            missing_mandatory = [c for c in mandatory_cols_by_entity[entity] if c not in cols]
            if missing_mandatory:
                SchemaEvolutionManager.record_event(
                    entity=entity,
                    layer="SILVER",
                    change_type="MISSING_REQUIRED_COLUMN",
                    prev_schema=json.dumps(mandatory_cols_by_entity[entity]),
                    curr_schema=json.dumps(list(cols)),
                    is_compatible=False,
                )
                raise ValueError(
                    f"Breaking schema violation in entity '{entity}': Required column '{missing_mandatory[0]}' is missing from incoming data."
                )

        if entity == "orders":
            # 1. Quarantine invalid records (negative amounts, invalid statuses, null IDs)
            bad_orders = con.execute("""
                SELECT * FROM raw_bronze
                WHERE order_id IS NULL
                   OR customer_id IS NULL
                   OR CAST(total_amount AS DOUBLE) < 0
                   OR UPPER(order_status) NOT IN ('COMPLETED', 'SHIPPED', 'PROCESSING', 'CANCELLED', 'REFUNDED')
            """).df()

            if not bad_orders.empty:
                quarantined_count = QuarantineManager.quarantine_records(
                    invalid_rows=bad_orders.to_dict(orient="records"),
                    entity="orders",
                    batch_id=batch_id,
                    error_category="DATA_VALIDATION_ERROR",
                    default_error_message="Order failed constraints (null key, negative total, or invalid status)",
                    id_field="order_id",
                )

            # 2. Filter valid, cleanse, cast types, and deduplicate
            currency_c = (
                "COALESCE(UPPER(currency), 'USD') AS currency"
                if "currency" in cols
                else "'USD' AS currency"
            )
            subtotal_c = (
                "ROUND(CAST(subtotal_amount AS DOUBLE), 2) AS subtotal_amount"
                if "subtotal_amount" in cols
                else "ROUND(CAST(total_amount AS DOUBLE), 2) AS subtotal_amount"
            )
            tax_c = (
                "ROUND(COALESCE(CAST(tax_amount AS DOUBLE), 0.0), 2) AS tax_amount"
                if "tax_amount" in cols
                else "0.00 AS tax_amount"
            )
            shipping_c = (
                "ROUND(COALESCE(CAST(shipping_amount AS DOUBLE), 0.0), 2) AS shipping_amount"
                if "shipping_amount" in cols
                else "0.00 AS shipping_amount"
            )
            discount_c = (
                "ROUND(COALESCE(CAST(discount_amount AS DOUBLE), 0.0), 2) AS discount_amount"
                if "discount_amount" in cols
                else "0.00 AS discount_amount"
            )
            created_c = (
                "CAST(created_at AS TIMESTAMP) AS created_at"
                if "created_at" in cols
                else "CURRENT_TIMESTAMP AS created_at"
            )
            updated_c = (
                "CAST(updated_at AS TIMESTAMP) AS updated_at"
                if "updated_at" in cols
                else "CURRENT_TIMESTAMP AS updated_at"
            )
            ingested_c = (
                "_ingested_at" if "_ingested_at" in cols else "CURRENT_TIMESTAMP AS _ingested_at"
            )
            batch_c = "_batch_id" if "_batch_id" in cols else f"'{batch_id}' AS _batch_id"

            std_orders = {
                "order_id",
                "customer_id",
                "order_status",
                "currency",
                "subtotal_amount",
                "tax_amount",
                "shipping_amount",
                "discount_amount",
                "total_amount",
                "created_at",
                "updated_at",
                "_ingested_at",
                "_batch_id",
            }
            extra_order_cols = [c for c in cols if not c.startswith("_") and c not in std_orders]
            extra_order_sql = (", " + ", ".join(extra_order_cols)) if extra_order_cols else ""

            valid_sql = f"""
                SELECT
                    order_id,
                    customer_id,
                    UPPER(order_status) AS order_status,
                    {currency_c},
                    {subtotal_c},
                    {tax_c},
                    {shipping_c},
                    {discount_c},
                    ROUND(CAST(total_amount AS DOUBLE), 2) AS total_amount,
                    {created_c},
                    {updated_c},
                    {ingested_c},
                    {batch_c}{extra_order_sql}
                FROM raw_bronze
                WHERE order_id IS NOT NULL
                  AND customer_id IS NOT NULL
                  AND CAST(total_amount AS DOUBLE) >= 0
                  AND UPPER(order_status) IN ('COMPLETED', 'SHIPPED', 'PROCESSING', 'CANCELLED', 'REFUNDED')
            """
            con.execute(f"CREATE TABLE valid_clean AS {valid_sql}")

            # 3. Deterministic window deduplication (order_id, latest updated_at, tie-break _ingested_at DESC)
            dedup_sql = """
                SELECT * EXCLUDE (row_num),
                       CURRENT_TIMESTAMP AS _silver_processed_at,
                       '1.0.0' AS _silver_version
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER(
                               PARTITION BY order_id
                               ORDER BY updated_at DESC NULLS LAST, _ingested_at DESC NULLS LAST
                           ) as row_num
                    FROM valid_clean
                )
                WHERE row_num = 1
            """
            clean_df = con.execute(dedup_sql).df()

        elif entity == "order_items":
            bad_items = con.execute("""
                SELECT * FROM raw_bronze
                WHERE order_id IS NULL
                   OR product_id IS NULL
                   OR CAST(quantity AS INTEGER) < 1
                   OR CAST(unit_price AS DOUBLE) < 0
            """).df()

            if not bad_items.empty:
                quarantined_count = QuarantineManager.quarantine_records(
                    invalid_rows=bad_items.to_dict(orient="records"),
                    entity="order_items",
                    batch_id=batch_id,
                    error_category="DATA_VALIDATION_ERROR",
                    default_error_message="Order item failed constraints (quantity < 1 or negative unit price)",
                    id_field="order_id",
                )

            total_p_c = (
                "ROUND(CAST(total_price AS DOUBLE), 2) AS total_price"
                if "total_price" in cols
                else "ROUND(CAST(quantity AS INTEGER) * CAST(unit_price AS DOUBLE), 2) AS total_price"
            )
            created_c = (
                "CAST(created_at AS TIMESTAMP) AS created_at"
                if "created_at" in cols
                else "CURRENT_TIMESTAMP AS created_at"
            )
            ingested_c = (
                "_ingested_at" if "_ingested_at" in cols else "CURRENT_TIMESTAMP AS _ingested_at"
            )
            batch_c = "_batch_id" if "_batch_id" in cols else f"'{batch_id}' AS _batch_id"

            valid_sql = f"""
                SELECT
                    md5(order_id || '_' || product_id) AS order_item_sk,
                    order_id,
                    product_id,
                    CAST(quantity AS INTEGER) AS quantity,
                    ROUND(CAST(unit_price AS DOUBLE), 2) AS unit_price,
                    {total_p_c},
                    {created_c},
                    {ingested_c},
                    {batch_c}
                FROM raw_bronze
                WHERE order_id IS NOT NULL
                  AND product_id IS NOT NULL
                  AND CAST(quantity AS INTEGER) >= 1
                  AND CAST(unit_price AS DOUBLE) >= 0
            """
            con.execute(f"CREATE TABLE valid_clean AS {valid_sql}")

            dedup_sql = """
                SELECT * EXCLUDE (row_num),
                       CURRENT_TIMESTAMP AS _silver_processed_at,
                       '1.0.0' AS _silver_version
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER(
                               PARTITION BY order_item_sk
                               ORDER BY _ingested_at DESC NULLS LAST
                           ) as row_num
                    FROM valid_clean
                )
                WHERE row_num = 1
            """
            clean_df = con.execute(dedup_sql).df()

        elif entity == "customers":
            email_check = "OR email IS NULL OR email NOT LIKE '%@%.%'" if "email" in cols else ""
            bad_customers = con.execute(f"""
                SELECT * FROM raw_bronze
                WHERE customer_id IS NULL
                   {email_check}
            """).df()

            if not bad_customers.empty:
                quarantined_count = QuarantineManager.quarantine_records(
                    invalid_rows=bad_customers.to_dict(orient="records"),
                    entity="customers",
                    batch_id=batch_id,
                    error_category="SCHEMA_VIOLATION",
                    default_error_message="Customer record failed validation (missing ID or invalid email format)",
                    id_field="customer_id",
                )

            fn_c = (
                "TRIM(first_name) AS first_name" if "first_name" in cols else "NULL AS first_name"
            )
            ln_c = "TRIM(last_name) AS last_name" if "last_name" in cols else "NULL AS last_name"
            city_c = "city" if "city" in cols else "NULL AS city"
            state_c = "state" if "state" in cols else "NULL AS state"
            country_c = (
                "COALESCE(country, 'USA') AS country" if "country" in cols else "'USA' AS country"
            )
            segment_c = (
                "COALESCE(UPPER(customer_segment), 'REGULAR') AS customer_segment"
                if "customer_segment" in cols
                else "'REGULAR' AS customer_segment"
            )
            signup_c = (
                "CAST(signup_date AS DATE) AS signup_date"
                if "signup_date" in cols
                else "CURRENT_DATE AS signup_date"
            )
            active_c = (
                "COALESCE(CAST(is_active AS BOOLEAN), true) AS is_active"
                if "is_active" in cols
                else "true AS is_active"
            )
            created_c = (
                "CAST(created_at AS TIMESTAMP) AS created_at"
                if "created_at" in cols
                else "CURRENT_TIMESTAMP AS created_at"
            )
            updated_c = (
                "CAST(updated_at AS TIMESTAMP) AS updated_at"
                if "updated_at" in cols
                else "CURRENT_TIMESTAMP AS updated_at"
            )
            ingested_c = (
                "_ingested_at" if "_ingested_at" in cols else "CURRENT_TIMESTAMP AS _ingested_at"
            )
            batch_c = "_batch_id" if "_batch_id" in cols else f"'{batch_id}' AS _batch_id"

            std_custs = {
                "customer_id",
                "first_name",
                "last_name",
                "email",
                "city",
                "state",
                "country",
                "customer_segment",
                "signup_date",
                "is_active",
                "created_at",
                "updated_at",
                "_ingested_at",
                "_batch_id",
            }
            extra_cust_cols = [c for c in cols if not c.startswith("_") and c not in std_custs]
            extra_cust_sql = (", " + ", ".join(extra_cust_cols)) if extra_cust_cols else ""

            valid_sql = f"""
                SELECT
                    customer_id,
                    {fn_c},
                    {ln_c},
                    LOWER(TRIM(email)) AS email,
                    {city_c},
                    {state_c},
                    {country_c},
                    {segment_c},
                    {signup_c},
                    {active_c},
                    {created_c},
                    {updated_c},
                    {ingested_c},
                    {batch_c}{extra_cust_sql}
                FROM raw_bronze
                WHERE customer_id IS NOT NULL
                  AND email IS NOT NULL
                  AND email LIKE '%@%.%'
            """
            con.execute(f"CREATE TABLE valid_clean AS {valid_sql}")

            dedup_sql = """
                SELECT * EXCLUDE (row_num),
                       CURRENT_TIMESTAMP AS _silver_processed_at,
                       '1.0.0' AS _silver_version
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER(
                               PARTITION BY customer_id
                               ORDER BY updated_at DESC NULLS LAST, _ingested_at DESC NULLS LAST
                           ) as row_num
                    FROM valid_clean
                )
                WHERE row_num = 1
            """
            clean_df = con.execute(dedup_sql).df()

        elif entity == "products":
            bad_prods = con.execute("""
                SELECT * FROM raw_bronze
                WHERE product_id IS NULL
                   OR CAST(retail_price AS DOUBLE) < 0
                   OR (cost_price IS NOT NULL AND CAST(cost_price AS DOUBLE) < 0)
            """).df()

            if not bad_prods.empty:
                quarantined_count = QuarantineManager.quarantine_records(
                    invalid_rows=bad_prods.to_dict(orient="records"),
                    entity="products",
                    batch_id=batch_id,
                    error_category="DATA_VALIDATION_ERROR",
                    default_error_message="Product failed constraints (null ID or negative price/cost)",
                    id_field="product_id",
                )

            brand_c = "brand" if "brand" in cols else "NULL AS brand"
            cost_c = (
                "ROUND(CAST(cost_price AS DOUBLE), 2) AS cost_price"
                if "cost_price" in cols
                else "ROUND(CAST(retail_price AS DOUBLE) * 0.6, 2) AS cost_price"
            )
            price_band_c = (
                """CASE
                        WHEN price_band IS NOT NULL THEN price_band
                        WHEN CAST(retail_price AS DOUBLE) < 50 THEN 'BUDGET'
                        WHEN CAST(retail_price AS DOUBLE) < 150 THEN 'MID_RANGE'
                        WHEN CAST(retail_price AS DOUBLE) < 400 THEN 'PREMIUM'
                        ELSE 'LUXURY'
                    END AS price_band"""
                if "price_band" in cols
                else """CASE
                        WHEN CAST(retail_price AS DOUBLE) < 50 THEN 'BUDGET'
                        WHEN CAST(retail_price AS DOUBLE) < 150 THEN 'MID_RANGE'
                        WHEN CAST(retail_price AS DOUBLE) < 400 THEN 'PREMIUM'
                        ELSE 'LUXURY'
                    END AS price_band"""
            )
            status_c = (
                "COALESCE(UPPER(status), 'ACTIVE') AS status"
                if "status" in cols
                else "'ACTIVE' AS status"
            )
            created_c = (
                "CAST(created_at AS TIMESTAMP) AS created_at"
                if "created_at" in cols
                else "CURRENT_TIMESTAMP AS created_at"
            )
            updated_c = (
                "CAST(updated_at AS TIMESTAMP) AS updated_at"
                if "updated_at" in cols
                else "CURRENT_TIMESTAMP AS updated_at"
            )
            ingested_c = (
                "_ingested_at" if "_ingested_at" in cols else "CURRENT_TIMESTAMP AS _ingested_at"
            )
            batch_c = "_batch_id" if "_batch_id" in cols else f"'{batch_id}' AS _batch_id"

            valid_sql = f"""
                SELECT
                    product_id,
                    TRIM(product_name) AS product_name,
                    category,
                    {brand_c},
                    {cost_c},
                    ROUND(CAST(retail_price AS DOUBLE), 2) AS retail_price,
                    {price_band_c},
                    {status_c},
                    {created_c},
                    {updated_c},
                    {ingested_c},
                    {batch_c}
                FROM raw_bronze
                WHERE product_id IS NOT NULL
                  AND CAST(retail_price AS DOUBLE) >= 0
            """
            con.execute(f"CREATE TABLE valid_clean AS {valid_sql}")

            dedup_sql = """
                SELECT * EXCLUDE (row_num),
                       CURRENT_TIMESTAMP AS _silver_processed_at,
                       '1.0.0' AS _silver_version
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER(
                               PARTITION BY product_id
                               ORDER BY updated_at DESC NULLS LAST, _ingested_at DESC NULLS LAST
                           ) as row_num
                    FROM valid_clean
                )
                WHERE row_num = 1
            """
            clean_df = con.execute(dedup_sql).df()

        elif entity == "payments":
            bad_payments = con.execute("""
                SELECT * FROM raw_bronze
                WHERE payment_id IS NULL
                   OR order_id IS NULL
                   OR CAST(amount AS DOUBLE) < 0
            """).df()

            if not bad_payments.empty:
                quarantined_count = QuarantineManager.quarantine_records(
                    invalid_rows=bad_payments.to_dict(orient="records"),
                    entity="payments",
                    batch_id=batch_id,
                    error_category="DATA_VALIDATION_ERROR",
                    default_error_message="Payment record failed constraints (null ID or negative amount)",
                    id_field="payment_id",
                )

            method_c = (
                "UPPER(payment_method) AS payment_method"
                if "payment_method" in cols
                else "'CREDIT_CARD' AS payment_method"
            )
            status_c = (
                "UPPER(payment_status) AS payment_status"
                if "payment_status" in cols
                else "'CAPTURED' AS payment_status"
            )
            refund_c = (
                "COALESCE(CAST(is_refund AS BOOLEAN), false) AS is_refund"
                if "is_refund" in cols
                else "false AS is_refund"
            )
            created_c = (
                "CAST(created_at AS TIMESTAMP) AS created_at"
                if "created_at" in cols
                else "CURRENT_TIMESTAMP AS created_at"
            )
            ingested_c = (
                "_ingested_at" if "_ingested_at" in cols else "CURRENT_TIMESTAMP AS _ingested_at"
            )
            batch_c = "_batch_id" if "_batch_id" in cols else f"'{batch_id}' AS _batch_id"

            valid_sql = f"""
                SELECT
                    payment_id,
                    order_id,
                    {method_c},
                    {status_c},
                    ROUND(CAST(amount AS DOUBLE), 2) AS amount,
                    {refund_c},
                    {created_c},
                    {ingested_c},
                    {batch_c}
                FROM raw_bronze
                WHERE payment_id IS NOT NULL
                  AND order_id IS NOT NULL
                  AND CAST(amount AS DOUBLE) >= 0
            """
            con.execute(f"CREATE TABLE valid_clean AS {valid_sql}")

            dedup_sql = """
                SELECT * EXCLUDE (row_num),
                       CURRENT_TIMESTAMP AS _silver_processed_at,
                       '1.0.0' AS _silver_version
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER(
                               PARTITION BY payment_id
                               ORDER BY created_at DESC NULLS LAST, _ingested_at DESC NULLS LAST
                           ) as row_num
                    FROM valid_clean
                )
                WHERE row_num = 1
            """
            clean_df = con.execute(dedup_sql).df()

        else:
            # Fallback pass-through for other entities (e.g. events)
            clean_df = con.execute("SELECT * FROM raw_bronze").df()

        # Evaluate Schema Evolution
        is_compat, changes = SchemaEvolutionManager.evaluate_schema(
            entity=entity,
            df=clean_df,
            layer="SILVER",
        )
        if not is_compat:
            breaking = [c.get("details", "") for c in changes if not c.get("is_compatible", True)]
            raise ValueError(
                f"Breaking schema violation in entity '{entity}': {'; '.join(breaking)}"
            )

        # Merge with existing Silver data if present (Idempotent Upsert)
        existing_silver_file = config.SILVER_DIR / entity / "data.parquet"
        if existing_silver_file.exists():
            con.register("new_clean", clean_df)
            con.execute(
                f"CREATE VIEW existing_silver AS SELECT * FROM read_parquet('{existing_silver_file}')"
            )

            # Determine key for union deduplication
            key_col = (
                "order_id"
                if entity == "orders"
                else (
                    "customer_id"
                    if entity == "customers"
                    else (
                        "product_id"
                        if entity == "products"
                        else ("payment_id" if entity == "payments" else "order_item_sk")
                    )
                )
            )

            ts_col = (
                "created_at"
                if entity == "payments"
                else ("_ingested_at" if entity == "order_items" else "updated_at")
            )

            merge_sql = f"""
                SELECT * EXCLUDE (row_num)
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER(
                               PARTITION BY {key_col}
                               ORDER BY {ts_col} DESC NULLS LAST, _ingested_at DESC NULLS LAST
                           ) as row_num
                    FROM (
                        SELECT * FROM existing_silver
                        UNION ALL BY NAME
                        SELECT * FROM new_clean
                    )
                )
                WHERE row_num = 1
            """
            clean_df = con.execute(merge_sql).df()

        # Write cleansed dataset to Silver Parquet
        target_path = LakehouseFS.write_silver_dataset(
            df=clean_df,
            entity=entity,
            mode="overwrite",
        )

        rows_out = len(clean_df)
        logger.info(
            "Silver transformation for '%s' complete: rows_in=%d, rows_out=%d, quarantined=%d",
            entity,
            rows_in,
            rows_out,
            quarantined_count,
        )

        return {
            "entity": entity,
            "rows_in": rows_in,
            "rows_out": rows_out,
            "rows_rejected": quarantined_count,
            "target_path": str(target_path),
            "is_compatible": is_compat,
        }
