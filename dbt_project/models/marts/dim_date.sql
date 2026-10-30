{{ config(materialized='table') }}

WITH date_spine AS (
    SELECT generate_series(
        '2025-01-01'::date,
        '2027-12-31'::date,
        '1 day'::interval
    )::date AS full_date
)
SELECT 
    CAST(TO_CHAR(full_date, 'YYYYMMDD') AS INTEGER) AS date_key,
    full_date,
    EXTRACT(YEAR FROM full_date)::INTEGER AS year,
    EXTRACT(QUARTER FROM full_date)::INTEGER AS quarter,
    EXTRACT(MONTH FROM full_date)::INTEGER AS month,
    TO_CHAR(full_date, 'Month') AS month_name,
    EXTRACT(DAY FROM full_date)::INTEGER AS day_of_month,
    EXTRACT(DOW FROM full_date)::INTEGER AS day_of_week,
    TO_CHAR(full_date, 'Day') AS day_name,
    EXTRACT(WEEK FROM full_date)::INTEGER AS week_of_year,
    CASE WHEN EXTRACT(DOW FROM full_date) IN (0, 6) THEN true ELSE false END AS is_weekend
FROM date_spine
