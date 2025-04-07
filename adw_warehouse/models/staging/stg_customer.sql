
{{ config(materialized='view') }}

SELECT 
    "CustomerID"     AS customer_id,
    "PersonID"       AS person_id,
    "StoreID"        AS store_id,
    "TerritoryID"    AS territory_id,
    "AccountNumber"  AS account_number,
    "rowguid"        AS rowguid,
    "ModifiedDate"   AS modified_date
FROM {{ source('staging', 'STG_ADW_Customer') }}
