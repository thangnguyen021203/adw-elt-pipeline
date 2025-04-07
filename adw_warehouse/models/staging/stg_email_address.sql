
{{ config(materialized='view') }}

SELECT 
    "BusinessEntityID" AS business_entity_id,
    "EmailAddress" AS email_address,
    "rowguid",
    "ModifiedDate" AS modified_date
FROM {{ source('staging', 'STG_ADW_EmailAddress') }}
