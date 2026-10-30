WITH source AS (
    SELECT * FROM {{ source('silver', 'order_items') }}
)
SELECT 
    order_item_sk,
    order_id,
    product_id,
    CAST(quantity AS INTEGER) AS quantity,
    {{ money_round('unit_price') }} AS unit_price,
    {{ money_round('total_price') }} AS total_price,
    CAST(created_at AS TIMESTAMP) AS created_at
FROM source
