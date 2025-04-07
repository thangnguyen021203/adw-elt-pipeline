
{{ config(materialized='view') }}

SELECT 
    "BusinessEntityID" AS business_entity_id,
    "TerritoryID" AS territory_id,
    "SalesQuota" AS sales_quota,
    "Bonus" AS bonus,
    "CommissionPct" AS commission_pct,
    "SalesYTD" AS sales_ytd,
    "SalesLastYear" AS sales_last_year,
    "rowguid" AS rowguid,
    "ModifiedDate" AS modified_date

FROM {{ source('staging', 'STG_ADW_SalesPerson') }}
