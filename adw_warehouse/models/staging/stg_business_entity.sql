
{{ config(materialized='view') }}

SELECT 
    "BusinessEntityID" AS business_entity_id,
    "rowguid"            AS rowguid,
    "ModifiedDate"     AS modified_date
FROM {{ source('staging', 'STG_ADW_BusinessEntity') }}
