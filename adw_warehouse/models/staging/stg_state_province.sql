
{{ config(materialized='view') }}

SELECT 
    "StateProvinceID"           AS state_province_id,
    "StateProvinceCode"         AS state_province_code,
    "CountryRegionCode"         AS country_region_code,
    "IsOnlyStateProvinceFlag"   AS is_only_state_province_flag,
    "Name"                      AS name,
    "TerritoryID"               AS territory_id,
    "rowguid"                     AS rowguid,
    "ModifiedDate"              AS modified_date
FROM {{ source('staging', 'STG_ADW_StateProvince') }}
