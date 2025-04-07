
{{ config(materialized='view') }}

SELECT 
    "AddressID"         AS address_id,
    "AddressLine1"      AS address_line_1,
    "AddressLine2"      AS address_line_2,
    "City"              AS city,
    "StateProvinceID"   AS state_province_id,
    "PostalCode"        AS postal_code,
    "SpatialLocation"   AS spatial_location,
    "rowguid"             AS rowguid,
    "ModifiedDate"      AS modified_date
FROM {{ source('staging', 'STG_ADW_Address') }}
