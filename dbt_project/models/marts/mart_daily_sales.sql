{{ config(materialized='table') }}

WITH daily_orders AS (
    SELECT 
        order_date_key AS date_key,
        order_created_at::date AS sale_date,
        COUNT(order_sk) AS total_orders,
        SUM(CASE WHEN order_status IN ('COMPLETED', 'SHIPPED') THEN 1 ELSE 0 END) AS completed_orders,
        ROUND(SUM(total_amount), 2) AS gross_revenue,
        ROUND(SUM(tax_amount), 2) AS tax_collected,
        ROUND(SUM(shipping_amount), 2) AS shipping_collected,
        ROUND(SUM(discount_amount), 2) AS discounts_given,
        COUNT(DISTINCT customer_sk) AS unique_customers
    FROM {{ ref('fact_orders') }}
    GROUP BY 1, 2
),
daily_refunds AS (
    SELECT 
        payment_date_key AS date_key,
        COUNT(payment_sk) AS refund_count,
        ROUND(SUM(amount), 2) AS refund_amount
    FROM {{ ref('fact_payments') }}
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
