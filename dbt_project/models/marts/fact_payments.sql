{{ config(materialized='table') }}

SELECT 
    MD5(p.payment_id) AS payment_sk,
    p.payment_id,
    fo.order_sk,
    p.payment_method,
    p.payment_status,
    p.amount,
    p.is_refund,
    CAST(TO_CHAR(p.created_at::date, 'YYYYMMDD') AS INTEGER) AS payment_date_key,
    p.created_at
FROM {{ ref('stg_payments') }} p
JOIN {{ ref('fact_orders') }} fo ON p.order_id = fo.order_id
