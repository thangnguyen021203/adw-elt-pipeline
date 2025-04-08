
{{ config(materialized='view') }}

SELECT 
    "CurrencyCode" AS currency_code,
    "Name" AS name,
    "ModifiedDate" AS modified_date

FROM {{ source('staging', 'STG_ADW_Currency') }}
