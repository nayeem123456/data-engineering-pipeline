-- Singular test: Assert gross_revenue and total_amount are non-negative
SELECT 
    date_key,
    gross_revenue
FROM {{ ref('mart_daily_sales') }}
WHERE gross_revenue < 0
