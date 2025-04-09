{{ config(materialized='table') }}

WITH base AS (
  SELECT
    full_datetime,

    -- Date part
    CAST(full_datetime AS DATE)            AS full_date,
    EXTRACT(YEAR FROM full_datetime)       AS year,
    EXTRACT(QUARTER FROM full_datetime)    AS quarter,
    EXTRACT(MONTH FROM full_datetime)      AS month,
    EXTRACT(DAY FROM full_datetime)        AS day,
    WEEKOFYEAR(full_datetime)              AS week_of_year,
    DAYOFWEEK(full_datetime)               AS day_of_week,
    CASE 
      WHEN DAYOFWEEK(full_datetime) IN (6, 7) THEN TRUE 
      ELSE FALSE 
    END                                    AS is_weekend,
    FALSE                                  AS is_holiday,
    CASE 
      WHEN MONTH(full_datetime) IN (12, 1, 2) THEN 'Winter'
      WHEN MONTH(full_datetime) IN (3, 4, 5) THEN 'Spring'
      WHEN MONTH(full_datetime) IN (6, 7, 8) THEN 'Summer'
      WHEN MONTH(full_datetime) IN (9, 10, 11) THEN 'Fall'
    END                                    AS season,

    -- Time part
    EXTRACT(HOUR FROM full_datetime)       AS hour,
    CASE 
      WHEN EXTRACT(HOUR FROM full_datetime) < 12 THEN TRUE 
      ELSE FALSE 
    END                                    AS is_am,
    CASE 
      WHEN EXTRACT(HOUR FROM full_datetime) BETWEEN 5 AND 11 THEN 'Morning'
      WHEN EXTRACT(HOUR FROM full_datetime) BETWEEN 12 AND 17 THEN 'Afternoon'
      WHEN EXTRACT(HOUR FROM full_datetime) BETWEEN 18 AND 21 THEN 'Evening'
      ELSE 'Night'
    END                                    AS time_of_day

  FROM {{ ref('stg_datetime_generator') }}
)

SELECT
  ROW_NUMBER() OVER (ORDER BY full_datetime) AS datetime_key,
  full_datetime,
  full_date,
  year,
  quarter,
  month,
  day,
  day_of_week,
  week_of_year,
  is_weekend,
  is_holiday,
  season,
  hour,
  is_am,
  time_of_day
FROM base
ORDER BY full_datetime
