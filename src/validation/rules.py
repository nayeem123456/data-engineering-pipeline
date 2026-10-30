"""
Data Quality Rule Specifications and Definitions.
Defines validation assertions across Bronze, Silver, and Gold lakehouse tiers.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass


@dataclass
class QualityRule:
    """Specification of an individual data quality check."""

    name: str
    layer: str  # BRONZE, SILVER, GOLD
    entity: str
    rule_type: str  # NULL_CHECK, UNIQUENESS, RANGE, CATEGORICAL, RECONCILIATION, TIMELINESS
    severity: str  # CRITICAL, WARNING
    description: str
    sql_check: str | None = None
    python_check: Callable | None = None


# Standard Lakehouse Quality Ruleset
RULES: list[QualityRule] = [
    # --------------------------------------------------------------------------
    # Silver Rules
    # --------------------------------------------------------------------------
    QualityRule(
        name="silver_orders_unique_key",
        layer="SILVER",
        entity="orders",
        rule_type="UNIQUENESS",
        severity="CRITICAL",
        description="Verify order_id business key is strictly unique after deduplication.",
        sql_check="""
            SELECT COUNT(*) - COUNT(DISTINCT order_id) AS failed_records
            FROM silver_orders
        """,
    ),
    QualityRule(
        name="silver_orders_no_null_keys",
        layer="SILVER",
        entity="orders",
        rule_type="NULL_CHECK",
        severity="CRITICAL",
        description="Ensure critical order identifiers and totals are never NULL.",
        sql_check="""
            SELECT COUNT(*) AS failed_records
            FROM silver_orders
            WHERE order_id IS NULL OR customer_id IS NULL OR total_amount IS NULL
        """,
    ),
    QualityRule(
        name="silver_orders_non_negative_amounts",
        layer="SILVER",
        entity="orders",
        rule_type="RANGE",
        severity="CRITICAL",
        description="Ensure monetary amounts are non-negative.",
        sql_check="""
            SELECT COUNT(*) AS failed_records
            FROM silver_orders
            WHERE total_amount < 0 OR subtotal_amount < 0
        """,
    ),
    QualityRule(
        name="silver_orders_valid_status",
        layer="SILVER",
        entity="orders",
        rule_type="CATEGORICAL",
        severity="CRITICAL",
        description="Verify order_status matches accepted business values.",
        sql_check="""
            SELECT COUNT(*) AS failed_records
            FROM silver_orders
            WHERE order_status NOT IN ('COMPLETED', 'SHIPPED', 'PROCESSING', 'CANCELLED', 'REFUNDED')
        """,
    ),
    QualityRule(
        name="silver_orders_valid_timestamps",
        layer="SILVER",
        entity="orders",
        rule_type="TIMELINESS",
        severity="CRITICAL",
        description="Verify order timestamps are well-formed and created_at <= updated_at.",
        sql_check="""
            SELECT COUNT(*) AS failed_records
            FROM silver_orders
            WHERE created_at IS NULL
               OR updated_at IS NULL
               OR updated_at < created_at
        """,
    ),
    QualityRule(
        name="silver_orders_foreign_keys",
        layer="SILVER",
        entity="orders",
        rule_type="REFERENTIAL_INTEGRITY",
        severity="WARNING",
        description="Verify orders reference known customers if customer table is available.",
        sql_check="""
            SELECT COUNT(*) AS failed_records
            FROM silver_orders o
            WHERE o.customer_id IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM silver_customers c WHERE c.customer_id = o.customer_id)
        """,
    ),
    QualityRule(
        name="silver_orders_discount_bounds",
        layer="SILVER",
        entity="orders",
        rule_type="RANGE",
        severity="WARNING",
        description="Flag orders where discount exceeds total order value.",
        sql_check="""
            SELECT COUNT(*) AS failed_records
            FROM silver_orders
            WHERE discount_amount > total_amount AND total_amount > 0
        """,
    ),
    QualityRule(
        name="silver_customers_unique_key",
        layer="SILVER",
        entity="customers",
        rule_type="UNIQUENESS",
        severity="CRITICAL",
        description="Verify customer_id is unique.",
        sql_check="""
            SELECT COUNT(*) - COUNT(DISTINCT customer_id) AS failed_records
            FROM silver_customers
        """,
    ),
    QualityRule(
        name="silver_products_valid_prices",
        layer="SILVER",
        entity="products",
        rule_type="RANGE",
        severity="CRITICAL",
        description="Verify retail and cost prices are non-negative.",
        sql_check="""
            SELECT COUNT(*) AS failed_records
            FROM silver_products
            WHERE retail_price < 0 OR cost_price < 0
        """,
    ),
    QualityRule(
        name="silver_customer_segment_distribution",
        layer="SILVER",
        entity="customers",
        rule_type="CATEGORICAL",
        severity="INFO",
        description="Audit customer segment categorical values.",
        sql_check="""
            SELECT COUNT(*) AS failed_records
            FROM silver_customers
            WHERE customer_segment NOT IN ('VIP', 'REGULAR', 'NEW', 'DORMANT')
        """,
    ),
    # --------------------------------------------------------------------------
    # Gold Rules
    # --------------------------------------------------------------------------
    QualityRule(
        name="gold_dim_customers_unique_sk",
        layer="GOLD",
        entity="dim_customers",
        rule_type="UNIQUENESS",
        severity="CRITICAL",
        description="Surrogate key customer_sk must be unique in dim_customers.",
        sql_check="""
            SELECT COUNT(*) - COUNT(DISTINCT customer_sk) AS failed_records
            FROM gold_dim_customers
        """,
    ),
    QualityRule(
        name="gold_fact_orders_referential_integrity",
        layer="GOLD",
        entity="fact_orders",
        rule_type="REFERENTIAL_INTEGRITY",
        severity="CRITICAL",
        description="Every order must reference a valid customer in dim_customers.",
        sql_check="""
            SELECT COUNT(*) AS failed_records
            FROM gold_fact_orders fo
            LEFT JOIN gold_dim_customers dc ON fo.customer_sk = dc.customer_sk
            WHERE dc.customer_sk IS NULL
        """,
    ),
    QualityRule(
        name="gold_fact_order_items_referential_integrity",
        layer="GOLD",
        entity="fact_order_items",
        rule_type="REFERENTIAL_INTEGRITY",
        severity="CRITICAL",
        description="Every line item must reference a valid product and order in Gold.",
        sql_check="""
            SELECT COUNT(*) AS failed_records
            FROM gold_fact_order_items foi
            LEFT JOIN gold_dim_products dp ON foi.product_sk = dp.product_sk
            LEFT JOIN gold_fact_orders fo ON foi.order_sk = fo.order_sk
            WHERE dp.product_sk IS NULL OR fo.order_sk IS NULL
        """,
    ),
    QualityRule(
        name="gold_revenue_reconciliation",
        layer="GOLD",
        entity="mart_daily_sales",
        rule_type="RECONCILIATION",
        severity="CRITICAL",
        description="Total revenue in mart_daily_sales must match fact_orders sum within 0.05 tolerance.",
        sql_check="""
            WITH fact_sum AS (
                SELECT ROUND(SUM(total_amount), 2) AS fact_rev FROM gold_fact_orders
            ),
            mart_sum AS (
                SELECT ROUND(SUM(gross_revenue), 2) AS mart_rev FROM gold_mart_daily_sales
            )
            SELECT CASE
                WHEN ABS(COALESCE(fact_rev, 0) - COALESCE(mart_rev, 0)) <= 0.05 THEN 0
                ELSE 1
            END AS failed_records
            FROM fact_sum CROSS JOIN mart_sum
        """,
    ),
    QualityRule(
        name="gold_positive_margin_validation",
        layer="GOLD",
        entity="fact_order_items",
        rule_type="RANGE",
        severity="WARNING",
        description="Audit line-item margin to identify loss-leader or negative margin items.",
        sql_check="""
            SELECT COUNT(*) AS failed_records
            FROM gold_fact_order_items
            WHERE estimated_margin < 0
        """,
    ),
    QualityRule(
        name="gold_aggregate_item_consistency",
        layer="GOLD",
        entity="mart_product_performance",
        rule_type="RECONCILIATION",
        severity="CRITICAL",
        description="Aggregated product units sold must match fact_order_items total units.",
        sql_check="""
            WITH mart_units AS (
                SELECT COALESCE(SUM(units_sold), 0) AS m_units FROM gold_mart_product_performance
            ),
            fact_units AS (
                SELECT COALESCE(SUM(quantity), 0) AS f_units FROM gold_fact_order_items
            )
            SELECT CASE
                WHEN m_units = f_units THEN 0
                ELSE 1
            END AS failed_records
            FROM mart_units CROSS JOIN fact_units
        """,
    ),
]
