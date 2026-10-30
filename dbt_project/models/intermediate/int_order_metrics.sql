WITH items AS (
    SELECT 
        order_id,
        COUNT(order_item_sk) AS item_count,
        SUM(quantity) AS total_units
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
),
payments AS (
    SELECT 
        order_id,
        COUNT(payment_id) AS payment_count,
        SUM(CASE WHEN is_refund THEN amount ELSE 0 END) AS total_refunded
    FROM {{ ref('stg_payments') }}
    GROUP BY order_id
)
SELECT 
    o.order_id,
    o.customer_id,
    o.order_status,
    o.total_amount,
    COALESCE(i.item_count, 1) AS item_count,
    COALESCE(i.total_units, 1) AS total_units,
    COALESCE(p.payment_count, 0) AS payment_count,
    COALESCE(p.total_refunded, 0.0) AS total_refunded,
    o.created_at,
    o.updated_at
FROM {{ ref('stg_orders') }} o
LEFT JOIN items i ON o.order_id = i.order_id
LEFT JOIN payments p ON o.order_id = p.order_id
