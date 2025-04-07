
{{ config(materialized='view') }}

SELECT 
    "TerritoryID" AS territory_id,
    "Name" AS name,
    "CountryRegionCode" AS country_region_code,
    "Group" AS sales_territory_group,
    "SalesYTD" AS sales_ytd,
    "SalesLastYear" AS sales_last_year,
    "CostYTD" AS cost_ytd,
    "CostLastYear" AS cost_last_year,
    "rowguid" AS rowguid,
    "ModifiedDate" AS modified_date

FROM {{ source('staging', 'STG_ADW_SalesTerritory') }}
