
{{ config(materialized='view') }}

SELECT 
    "CurrencyRateID" AS currency_rate_id,
    "CurrencyRateDate" AS currency_rate_date,
    "FromCurrencyCode" AS from_currency_code,
    "ToCurrencyCode" AS to_currency_code,
    "AverageRate" AS average_rate,
    "EndOfDayRate" AS end_of_day_rate,
    "ModifiedDate" AS modified_date

FROM {{ source('staging', 'STG_ADW_CurrencyRate') }}
