{{ config(materialized='table') }}

SELECT 
    MD5(o.order_id) AS order_sk,
    o.order_id,
    CAST(TO_CHAR(o.created_at::date, 'YYYYMMDD') AS INTEGER) AS order_date_key,
    c.customer_sk,
    o.order_status,
    o.currency,
    o.subtotal_amount,
    o.tax_amount,
    o.shipping_amount,
    o.discount_amount,
    o.total_amount,
    COALESCE(i.item_count, 1) AS item_count,
    o.created_at AS order_created_at,
    o.updated_at AS order_updated_at
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('dim_customers') }} c ON o.customer_id = c.customer_id
LEFT JOIN (
    SELECT order_id, COUNT(*) AS item_count 
    FROM {{ ref('stg_order_items') }} 
    GROUP BY order_id
) i ON o.order_id = i.order_id
