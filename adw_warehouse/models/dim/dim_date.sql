{{ config(materialized='table') }}

WITH base AS (
  SELECT
    full_date,
    YEAR(full_date)       AS year,
    QUARTER(full_date)    AS quarter,
    MONTH(full_date)      AS month,
    DAY(full_date)        AS day,
    WEEKOFYEAR(full_date) AS week_of_year,
    CASE WHEN DAYOFWEEK(full_date) IN (6,7) THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE AS is_holiday,
    CASE 
      WHEN MONTH(full_date) IN (12, 1, 2) THEN 'Winter'
      WHEN MONTH(full_date) IN (3, 4, 5) THEN 'Spring'
      WHEN MONTH(full_date) IN (6, 7, 8) THEN 'Summer'
      WHEN MONTH(full_date) IN (9, 10, 11) THEN 'Fall'
    END AS season
  FROM {{ ref('stg_date_generator') }}
)

SELECT
  ROW_NUMBER() OVER (ORDER BY full_date) AS date_key,
  full_date,
  year,
  quarter,
  month,
  day,
  week_of_year,
  is_weekend,
  is_holiday,
  season
FROM base
ORDER BY full_date
