
{{ config(materialized='view') }}

SELECT 
    "BusinessEntityID" AS business_entity_id,
    "Name" AS name,
    "SalesPersonID" AS salesperson_id,
    "Demographics" AS demographics,
    "rowguid" AS rowguid,
    "ModifiedDate" AS modified_date

FROM {{ source('staging', 'STG_ADW_Store') }}
