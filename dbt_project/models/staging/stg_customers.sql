WITH source AS (
    SELECT * FROM {{ source('silver', 'customers') }}
)
SELECT 
    customer_id,
    first_name,
    last_name,
    email,
    city,
    state,
    country,
    customer_segment,
    CAST(signup_date AS DATE) AS signup_date,
    is_active,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(updated_at AS TIMESTAMP) AS updated_at
FROM source
