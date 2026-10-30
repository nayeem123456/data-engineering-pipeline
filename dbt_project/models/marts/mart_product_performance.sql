{{ config(materialized='table') }}

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
FROM {{ ref('dim_products') }} p
JOIN {{ ref('fact_order_items') }} foi ON p.product_sk = foi.product_sk
JOIN {{ ref('fact_orders') }} fo ON foi.order_sk = fo.order_sk
GROUP BY p.product_sk, p.product_id, p.product_name, p.category, p.price_band
ORDER BY gross_revenue DESC
