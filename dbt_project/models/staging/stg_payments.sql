WITH source AS (
    SELECT * FROM {{ source('silver', 'payments') }}
)
SELECT 
    payment_id,
    order_id,
    UPPER(payment_method) AS payment_method,
    UPPER(payment_status) AS payment_status,
    {{ money_round('amount') }} AS amount,
    CAST(is_refund AS BOOLEAN) AS is_refund,
    CAST(created_at AS TIMESTAMP) AS created_at
FROM source
