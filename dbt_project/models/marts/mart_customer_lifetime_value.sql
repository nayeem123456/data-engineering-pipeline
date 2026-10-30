{{ config(materialized='table') }}

SELECT 
    c.customer_sk,
    c.customer_id,
    TRIM(c.first_name || ' ' || COALESCE(c.last_name, '')) AS customer_name,
    c.customer_segment,
    (CURRENT_DATE - c.signup_date) AS tenure_days,
    MIN(fo.order_created_at::date) AS first_order_date,
    MAX(fo.order_created_at::date) AS last_order_date,
    COUNT(fo.order_sk) AS total_orders,
    ROUND(SUM(fo.total_amount), 2) AS total_spend,
    COALESCE(SUM(fo.item_count), 0) AS total_items_purchased,
    ROUND(SUM(fo.total_amount) / NULLIF(COUNT(fo.order_sk), 0), 2) AS average_order_value,
    CASE WHEN COUNT(fo.order_sk) > 1 THEN true ELSE false END AS is_repeat_customer
FROM {{ ref('dim_customers') }} c
JOIN {{ ref('fact_orders') }} fo ON c.customer_sk = fo.customer_sk
GROUP BY c.customer_sk, c.customer_id, c.first_name, c.last_name, c.customer_segment, c.signup_date
