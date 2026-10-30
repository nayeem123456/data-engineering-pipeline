{{ config(materialized='table') }}

SELECT 
    MD5(customer_id) AS customer_sk,
    customer_id,
    first_name,
    last_name,
    email,
    city,
    state,
    country,
    customer_segment,
    signup_date,
    is_active,
    created_at,
    updated_at
FROM {{ ref('stg_customers') }}
