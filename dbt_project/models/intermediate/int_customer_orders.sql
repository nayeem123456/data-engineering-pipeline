WITH orders AS (
    SELECT 
        customer_id,
        COUNT(order_id) AS lifetime_orders,
        SUM(total_amount) AS lifetime_spend,
        MIN(created_at) AS first_order_timestamp,
        MAX(created_at) AS most_recent_order_timestamp
    FROM {{ ref('stg_orders') }}
    WHERE order_status != 'CANCELLED'
    GROUP BY customer_id
)
SELECT 
    c.customer_id,
    c.customer_segment,
    COALESCE(o.lifetime_orders, 0) AS lifetime_orders,
    COALESCE(o.lifetime_spend, 0.0) AS lifetime_spend,
    o.first_order_timestamp,
    o.most_recent_order_timestamp,
    CASE WHEN COALESCE(o.lifetime_orders, 0) > 1 THEN true ELSE false END AS is_repeat_customer
FROM {{ ref('stg_customers') }} c
LEFT JOIN orders o ON c.customer_id = o.customer_id
