{{ config(materialized='table') }}

WITH base AS (
    SELECT 
        currency_rate_id,
        currency_rate_date,
        from_currency_code,
        to_currency_code,
        average_rate,
        end_of_day_rate
    FROM {{ ref('stg_currency_rate') }}
),

joined_dates AS (
    SELECT
        b.*,
        d.datetime_key AS rate_date_key
    FROM base b
    LEFT JOIN {{ ref('dim_date') }} d
        ON CAST(CAST(b.currency_rate_date AS DATE) AS TIMESTAMP_NTZ) = d.full_datetime
),

joined_currencies AS (
    SELECT
        j.*,
        dc_from.currency_key AS from_currency_key,
        dc_to.currency_key   AS to_currency_key
    FROM joined_dates j
    LEFT JOIN {{ ref('dim_currency') }} dc_from
        ON j.from_currency_code = dc_from.currency_code
    LEFT JOIN {{ ref('dim_currency') }} dc_to
        ON j.to_currency_code = dc_to.currency_code

)

SELECT
    currency_rate_id,
    rate_date_key,
    from_currency_key,
    to_currency_key,
    average_rate,
    end_of_day_rate
FROM joined_currencies
