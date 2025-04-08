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
        d1.datetime_key AS rate_date_key,
    FROM base b
    LEFT JOIN {{ ref('dim_date') }} d1
        ON CAST(CAST(b.currency_rate_date AS DATE) AS TIMESTAMP_NTZ) = d1.full_datetime
)

SELECT
    currency_rate_id,
    rate_date_key,
    from_currency_code,
    to_currency_code,
    average_rate,
    end_of_day_rate
FROM joined_dates
