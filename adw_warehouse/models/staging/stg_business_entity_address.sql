
{{ config(materialized='view') }}

SELECT
    "BusinessEntityID" AS business_entity_id,
    "AddressID"        AS address_id,
    "AddressTypeID"    AS address_type_id,
    "rowguid"            AS rowguid,
    "ModifiedDate"     AS modified_date
FROM {{ source('staging', 'STG_ADW_BusinessEntityAddress') }}
