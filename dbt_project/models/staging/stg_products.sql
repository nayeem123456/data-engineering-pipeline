WITH source AS (
    SELECT * FROM {{ source('silver', 'products') }}
)
SELECT 
    product_id,
    product_name,
    category,
    brand,
    {{ money_round('cost_price') }} AS cost_price,
    {{ money_round('retail_price') }} AS retail_price,
    price_band,
    status,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(updated_at AS TIMESTAMP) AS updated_at
FROM source
