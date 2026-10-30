-- ==============================================================================
-- 03_analytics_queries.sql
-- Production analytical queries and financial reconciliation reports
-- ==============================================================================

-- 1. Daily Executive Summary & Margin Report
SELECT 
    d.sale_date,
    d.total_orders,
    d.completed_orders,
    d.gross_revenue,
    d.refund_amount,
    d.net_revenue,
    d.average_order_value,
    d.unique_customers,
    ROUND((d.net_revenue / NULLIF(d.completed_orders, 0)), 2) AS net_revenue_per_completed_order
FROM gold.mart_daily_sales d
ORDER BY d.sale_date DESC;


-- 2. Financial Reconciliation: Fact Orders vs Daily Sales Mart
SELECT 
    'fact_orders' AS source_table,
    COUNT(order_sk) AS total_orders_count,
    ROUND(SUM(total_amount), 2) AS total_revenue_sum
FROM gold.fact_orders
WHERE order_status != 'CANCELLED'
UNION ALL
SELECT 
    'mart_daily_sales' AS source_table,
    SUM(total_orders) AS total_orders_count,
    ROUND(SUM(gross_revenue), 2) AS total_revenue_sum
FROM gold.mart_daily_sales;


-- 3. Top Products by Margin and Category Performance
SELECT 
    p.category,
    p.product_name,
    p.price_band,
    p.units_sold,
    p.gross_revenue,
    p.total_margin,
    p.margin_percentage,
    RANK() OVER (PARTITION BY p.category ORDER BY p.total_margin DESC) AS category_margin_rank
FROM gold.mart_product_performance p
ORDER BY p.category, category_margin_rank;


-- 4. Customer Segmentation & Repeat Buyer Health
SELECT 
    c.customer_segment,
    COUNT(c.customer_sk) AS total_customers,
    SUM(CASE WHEN c.is_repeat_customer THEN 1 ELSE 0 END) AS repeat_customers_count,
    ROUND(100.0 * SUM(CASE WHEN c.is_repeat_customer THEN 1 ELSE 0 END) / COUNT(c.customer_sk), 2) AS repeat_customer_rate_pct,
    ROUND(AVG(c.tenure_days), 1) AS avg_tenure_days,
    ROUND(SUM(c.total_spend), 2) AS total_segment_spend,
    ROUND(AVG(c.total_spend), 2) AS avg_spend_per_customer
FROM gold.mart_customer_lifetime_value c
GROUP BY c.customer_segment
ORDER BY total_segment_spend DESC;


-- 5. Payment Channel Risk and Refund Rate Analysis
SELECT 
    r.payment_method,
    SUM(r.refund_count) AS total_refunds,
    ROUND(SUM(r.refund_amount), 2) AS total_refunded_dollars,
    ROUND(AVG(r.avg_refund_size), 2) AS mean_refund_size
FROM gold.mart_refund_analysis r
GROUP BY r.payment_method
ORDER BY total_refunded_dollars DESC;
