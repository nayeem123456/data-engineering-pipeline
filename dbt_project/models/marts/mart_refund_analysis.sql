{{ config(materialized='table') }}

SELECT 
    created_at::date AS refund_date,
    payment_method,
    COUNT(payment_sk) AS refund_count,
    ROUND(SUM(amount), 2) AS refund_amount,
    ROUND(AVG(amount), 2) AS avg_refund_size
FROM {{ ref('fact_payments') }}
WHERE is_refund = true OR payment_status = 'REFUNDED'
GROUP BY 1, 2
ORDER BY refund_date DESC
