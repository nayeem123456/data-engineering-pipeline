{{ config(materialized='table') }}

SELECT 
    MD5(product_id) AS product_sk,
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
FROM {{ ref('stg_products') }}
