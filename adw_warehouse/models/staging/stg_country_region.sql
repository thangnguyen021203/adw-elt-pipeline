
{{ config(materialized='view') }}

SELECT 
    "CountryRegionCode" AS country_region_code,
    "Name"              AS name,
    "ModifiedDate"      AS modified_date
FROM {{ source('staging', 'STG_ADW_CountryRegion') }}
