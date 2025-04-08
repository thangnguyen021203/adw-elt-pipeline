
{{ config(materialized='view') }}

SELECT 
    "CountryRegionCode" AS country_region_code,
    "CurrencyCode" AS currency_code,
    "ModifiedDate" AS modified_date

FROM {{ source('staging', 'STG_ADW_CountryRegionCurrency') }}
