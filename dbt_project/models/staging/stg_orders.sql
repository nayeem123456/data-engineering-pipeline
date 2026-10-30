WITH source AS (
    SELECT * FROM {{ source('silver', 'orders') }}
)
SELECT 
    order_id,
    customer_id,
    UPPER(order_status) AS order_status,
    currency,
    {{ money_round('subtotal_amount') }} AS subtotal_amount,
    {{ money_round('tax_amount') }} AS tax_amount,
    {{ money_round('shipping_amount') }} AS shipping_amount,
    {{ money_round('discount_amount') }} AS discount_amount,
    {{ money_round('total_amount') }} AS total_amount,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(updated_at AS TIMESTAMP) AS updated_at
FROM source
