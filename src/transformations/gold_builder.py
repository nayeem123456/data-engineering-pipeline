"""
Gold Layer Dimensional Modeling and Analytical Mart Generator.
Transforms conformed Silver datasets into dimensional Kimball star schemas
(dim_*, fact_*) and business marts, writes Gold Parquet tables, and syncs to PostgreSQL serving schema.
"""

from __future__ import annotations

import hashlib

import duckdb
from sqlalchemy import text

from src.common.config import config
from src.common.db import db_manager
from src.common.logger import get_logger
from src.monitoring.operational_metadata import OperationalMetadata
from src.storage.lakehouse_fs import LakehouseFS

logger = get_logger("platform.transformations.gold_builder")


class GoldDimensionalBuilder:
    """Builds dimensional Kimball star schema facts, dimensions, and analytical marts."""

    @classmethod
    def build_gold_layer(cls, batch_id: str) -> dict[str, int]:
        """
        Execute SQL transformations to build all Gold dimensional tables and marts.
        Returns a dictionary of generated table row counts.
        """
        con = duckdb.connect(database=":memory:")
        table_counts: dict[str, int] = {}

        # 1. Register Silver Parquet Tables
        silver_entities = ["orders", "order_items", "customers", "products", "payments"]
        for ent in silver_entities:
            p = config.SILVER_DIR / ent / "data.parquet"
            if not p.exists():
                logger.warning("Silver table '%s' missing. Skipping Gold build.", ent)
                return {}
            con.execute(f"CREATE VIEW silver_{ent} AS SELECT * FROM read_parquet('{p}')")

        # 2. Build Date Dimension (dim_date)
        dim_date_sql = """
            WITH date_spine AS (
                SELECT UNNEST(GENERATE_SERIES(
                    DATE '2025-01-01',
                    DATE '2027-12-31',
                    INTERVAL 1 DAY
                ))::DATE AS full_date
            )
            SELECT
                CAST(STRFTIME(full_date, '%Y%m%d') AS INTEGER) AS date_key,
                full_date,
                EXTRACT(YEAR FROM full_date)::INTEGER AS year,
                EXTRACT(QUARTER FROM full_date)::INTEGER AS quarter,
                EXTRACT(MONTH FROM full_date)::INTEGER AS month,
                STRFTIME(full_date, '%B') AS month_name,
                EXTRACT(DAY FROM full_date)::INTEGER AS day_of_month,
                EXTRACT(DOW FROM full_date)::INTEGER AS day_of_week,
                STRFTIME(full_date, '%A') AS day_name,
                EXTRACT(WEEK FROM full_date)::INTEGER AS week_of_year,
                CASE WHEN EXTRACT(DOW FROM full_date) IN (0, 6) THEN true ELSE false END AS is_weekend
            FROM date_spine
        """
        df_dim_date = con.execute(dim_date_sql).df()
        LakehouseFS.write_gold_table(df_dim_date, "dim_date")
        con.register("gold_dim_date", df_dim_date)
        table_counts["dim_date"] = len(df_dim_date)

        # 3. Build Customer Dimension (dim_customers)
        dim_cust_sql = """
            SELECT
                md5(customer_id) AS customer_sk,
                customer_id,
                first_name,
                last_name,
                email,
                city,
                state,
                country,
                customer_segment,
                signup_date,
                is_active,
                created_at,
                updated_at
            FROM silver_customers
        """
        df_dim_cust = con.execute(dim_cust_sql).df()
        LakehouseFS.write_gold_table(df_dim_cust, "dim_customers")
        con.register("gold_dim_customers", df_dim_cust)
        table_counts["dim_customers"] = len(df_dim_cust)

        # 4. Build Product Dimension (dim_products)
        dim_prod_sql = """
            SELECT
                md5(product_id) AS product_sk,
                product_id,
                product_name,
                category,
                brand,
                cost_price,
                retail_price,
                price_band,
                status,
                created_at,
                updated_at
            FROM silver_products
        """
        df_dim_prod = con.execute(dim_prod_sql).df()
        LakehouseFS.write_gold_table(df_dim_prod, "dim_products")
        con.register("gold_dim_products", df_dim_prod)
        table_counts["dim_products"] = len(df_dim_prod)

        # 5. Build Orders Fact (fact_orders)
        fact_orders_sql = """
            SELECT
                md5(o.order_id) AS order_sk,
                o.order_id,
                CAST(STRFTIME(CAST(o.created_at AS DATE), '%Y%m%d') AS INTEGER) AS order_date_key,
                c.customer_sk,
                o.order_status,
                o.currency,
                o.subtotal_amount,
                o.tax_amount,
                o.shipping_amount,
                o.discount_amount,
                o.total_amount,
                COALESCE(item_counts.cnt, 1) AS item_count,
                o.created_at AS order_created_at,
                o.updated_at AS order_updated_at
            FROM silver_orders o
            JOIN gold_dim_customers c ON o.customer_id = c.customer_id
            LEFT JOIN (
                SELECT order_id, COUNT(*) AS cnt
                FROM silver_order_items
                GROUP BY order_id
            ) item_counts ON o.order_id = item_counts.order_id
        """
        df_fact_orders = con.execute(fact_orders_sql).df()
        LakehouseFS.write_gold_table(df_fact_orders, "fact_orders")
        con.register("gold_fact_orders", df_fact_orders)
        table_counts["fact_orders"] = len(df_fact_orders)

        # 6. Build Order Items Fact (fact_order_items)
        fact_items_sql = """
            SELECT
                md5(oi.order_id || '_' || oi.product_id) AS order_item_sk,
                oi.order_id,
                fo.order_sk,
                p.product_sk,
                oi.quantity,
                oi.unit_price,
                oi.total_price,
                ROUND(oi.total_price - (oi.quantity * p.cost_price), 2) AS estimated_margin,
                oi.created_at
            FROM silver_order_items oi
            JOIN gold_fact_orders fo ON oi.order_id = fo.order_id
            JOIN gold_dim_products p ON oi.product_id = p.product_id
        """
        df_fact_items = con.execute(fact_items_sql).df()
        LakehouseFS.write_gold_table(df_fact_items, "fact_order_items")
        con.register("gold_fact_order_items", df_fact_items)
        table_counts["fact_order_items"] = len(df_fact_items)

        # 7. Build Payments Fact (fact_payments)
        fact_pay_sql = """
            SELECT
                md5(p.payment_id) AS payment_sk,
                p.payment_id,
                fo.order_sk,
                p.payment_method,
                p.payment_status,
                p.amount,
                p.is_refund,
                CAST(STRFTIME(CAST(p.created_at AS DATE), '%Y%m%d') AS INTEGER) AS payment_date_key,
                p.created_at
            FROM silver_payments p
            JOIN gold_fact_orders fo ON p.order_id = fo.order_id
        """
        df_fact_pay = con.execute(fact_pay_sql).df()
        LakehouseFS.write_gold_table(df_fact_pay, "fact_payments")
        con.register("gold_fact_payments", df_fact_pay)
        table_counts["fact_payments"] = len(df_fact_pay)

        # 8. Build Analytical Mart: mart_daily_sales
        mart_daily_sql = """
            WITH daily_orders AS (
                SELECT
                    CAST(STRFTIME(order_created_at::DATE, '%Y%m%d') AS INTEGER) AS date_key,
                    order_created_at::DATE AS sale_date,
                    COUNT(order_sk) AS total_orders,
                    SUM(CASE WHEN order_status IN ('COMPLETED', 'SHIPPED') THEN 1 ELSE 0 END) AS completed_orders,
                    ROUND(SUM(total_amount), 2) AS gross_revenue,
                    ROUND(SUM(tax_amount), 2) AS tax_collected,
                    ROUND(SUM(shipping_amount), 2) AS shipping_collected,
                    ROUND(SUM(discount_amount), 2) AS discounts_given,
                    COUNT(DISTINCT customer_sk) AS unique_customers
                FROM gold_fact_orders
                GROUP BY 1, 2
            ),
            daily_refunds AS (
                SELECT
                    payment_date_key AS date_key,
                    COUNT(payment_sk) AS refund_count,
                    ROUND(SUM(amount), 2) AS refund_amount
                FROM gold_fact_payments
                WHERE is_refund = true OR payment_status = 'REFUNDED'
                GROUP BY 1
            )
            SELECT
                d.date_key,
                d.sale_date,
                d.total_orders,
                d.completed_orders,
                d.gross_revenue,
                d.tax_collected,
                d.shipping_collected,
                d.discounts_given,
                COALESCE(r.refund_count, 0) AS refund_count,
                COALESCE(r.refund_amount, 0.00) AS refund_amount,
                ROUND(d.gross_revenue - COALESCE(r.refund_amount, 0.00), 2) AS net_revenue,
                d.unique_customers,
                ROUND(d.gross_revenue / NULLIF(d.total_orders, 0), 2) AS average_order_value
            FROM daily_orders d
            LEFT JOIN daily_refunds r ON d.date_key = r.date_key
            ORDER BY d.sale_date DESC
        """
        df_mart_daily = con.execute(mart_daily_sql).df()
        LakehouseFS.write_gold_table(df_mart_daily, "mart_daily_sales")
        con.register("gold_mart_daily_sales", df_mart_daily)
        table_counts["mart_daily_sales"] = len(df_mart_daily)

        # 9. Build Analytical Mart: mart_customer_lifetime_value
        mart_clv_sql = """
            SELECT
                c.customer_sk,
                c.customer_id,
                TRIM(c.first_name || ' ' || COALESCE(c.last_name, '')) AS customer_name,
                c.customer_segment,
                DATEDIFF('day', c.signup_date, CURRENT_DATE) AS tenure_days,
                MIN(fo.order_created_at::DATE) AS first_order_date,
                MAX(fo.order_created_at::DATE) AS last_order_date,
                COUNT(fo.order_sk) AS total_orders,
                ROUND(SUM(fo.total_amount), 2) AS total_spend,
                COALESCE(SUM(fo.item_count), 0) AS total_items_purchased,
                ROUND(SUM(fo.total_amount) / NULLIF(COUNT(fo.order_sk), 0), 2) AS average_order_value,
                CASE WHEN COUNT(fo.order_sk) > 1 THEN true ELSE false END AS is_repeat_customer
            FROM gold_dim_customers c
            JOIN gold_fact_orders fo ON c.customer_sk = fo.customer_sk
            GROUP BY c.customer_sk, c.customer_id, c.first_name, c.last_name, c.customer_segment, c.signup_date
        """
        df_mart_clv = con.execute(mart_clv_sql).df()
        LakehouseFS.write_gold_table(df_mart_clv, "mart_customer_lifetime_value")
        con.register("gold_mart_customer_lifetime_value", df_mart_clv)
        table_counts["mart_customer_lifetime_value"] = len(df_mart_clv)

        # 10. Build Analytical Mart: mart_product_performance
        mart_prod_sql = """
            SELECT
                p.product_sk,
                p.product_id,
                p.product_name,
                p.category,
                p.price_band,
                SUM(foi.quantity) AS units_sold,
                ROUND(SUM(foi.total_price), 2) AS gross_revenue,
                ROUND(SUM(foi.estimated_margin), 2) AS total_margin,
                ROUND((SUM(foi.estimated_margin) / NULLIF(SUM(foi.total_price), 0)) * 100, 2) AS margin_percentage,
                COUNT(DISTINCT fo.customer_sk) AS unique_buyers
            FROM gold_dim_products p
            JOIN gold_fact_order_items foi ON p.product_sk = foi.product_sk
            JOIN gold_fact_orders fo ON foi.order_sk = fo.order_sk
            GROUP BY p.product_sk, p.product_id, p.product_name, p.category, p.price_band
            ORDER BY gross_revenue DESC
        """
        df_mart_prod = con.execute(mart_prod_sql).df()
        LakehouseFS.write_gold_table(df_mart_prod, "mart_product_performance")
        con.register("gold_mart_product_performance", df_mart_prod)
        table_counts["mart_product_performance"] = len(df_mart_prod)

        # 11. Build Analytical Mart: mart_refund_analysis
        mart_refund_sql = """
            SELECT
                created_at::DATE AS refund_date,
                payment_method,
                COUNT(payment_sk) AS refund_count,
                ROUND(SUM(amount), 2) AS refund_amount,
                ROUND(AVG(amount), 2) AS avg_refund_size
            FROM gold_fact_payments
            WHERE is_refund = true OR payment_status = 'REFUNDED'
            GROUP BY 1, 2
            ORDER BY refund_date DESC
        """
        df_mart_refund = con.execute(mart_refund_sql).df()
        LakehouseFS.write_gold_table(df_mart_refund, "mart_refund_analysis")
        con.register("gold_mart_refund_analysis", df_mart_refund)
        table_counts["mart_refund_analysis"] = len(df_mart_refund)

        # Record published version metadata
        for table_name, count in table_counts.items():
            OperationalMetadata.record_published_version(
                dataset_name=table_name,
                layer="GOLD",
                version_tag=f"v_{batch_id}",
                row_count=count,
                storage_path=str(config.GOLD_DIR / table_name / "data.parquet"),
                checksum=hashlib.md5(f"{table_name}_{batch_id}_{count}".encode()).hexdigest(),
            )

        logger.info("Successfully built Gold layer: %s", table_counts)
        return table_counts

    @classmethod
    def sync_to_serving_database(cls) -> None:
        """Export Gold Parquet datasets into PostgreSQL / SQLite serving tables."""
        engine = db_manager.get_engine()
        is_sqlite = db_manager.is_sqlite()

        gold_tables = [
            "dim_date",
            "dim_customers",
            "dim_products",
            "fact_orders",
            "fact_order_items",
            "fact_payments",
            "mart_daily_sales",
            "mart_customer_lifetime_value",
            "mart_product_performance",
            "mart_refund_analysis",
        ]

        # First ensure schema tables exist in target DB
        if not is_sqlite:
            sql_file = config.PROJECT_ROOT / "sql" / "02_init_gold_schema.sql"
            if sql_file.exists():
                with open(sql_file, encoding="utf-8") as f:
                    ddl = f.read()
                with engine.connect() as conn:
                    conn.execute(text(ddl))
                    conn.commit()

        for tname in gold_tables:
            parquet_path = config.GOLD_DIR / tname / "data.parquet"
            if not parquet_path.exists():
                continue

            df = LakehouseFS.read_parquet(parquet_path)
            if df.empty:
                continue

            target_table = tname if is_sqlite else f"gold.{tname}"
            try:
                # Truncate and replace or upsert into serving table
                if is_sqlite:
                    df.to_sql(target_table, engine, if_exists="replace", index=False)
                else:
                    with engine.connect() as conn:
                        conn.execute(text(f"TRUNCATE TABLE {target_table} CASCADE;"))
                        conn.commit()
                    df.to_sql(tname, engine, schema="gold", if_exists="append", index=False)
                logger.info("Synced %d rows to serving table '%s'", len(df), target_table)
            except Exception as exc:
                logger.warning("Could not sync '%s' to serving DB: %s", target_table, exc)
