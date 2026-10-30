{{ config(materialized='table') }}

SELECT 
    MD5(CONCAT_WS('_', oi.order_id, oi.product_id)) AS order_item_sk,
    oi.order_id,
    fo.order_sk,
    p.product_sk,
    oi.quantity,
    oi.unit_price,
    oi.total_price,
    ROUND(oi.total_price - (oi.quantity * p.cost_price), 2) AS estimated_margin,
    oi.created_at
FROM {{ ref('stg_order_items') }} oi
JOIN {{ ref('fact_orders') }} fo ON oi.order_id = fo.order_id
JOIN {{ ref('dim_products') }} p ON oi.product_id = p.product_id
