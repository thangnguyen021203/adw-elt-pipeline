
{{ config(materialized='view') }}

SELECT 
    "SalesTaxRateID" AS sales_tax_rate_id,
    "StateProvinceID" AS state_province_id,
    "TaxType" AS tax_type,
    "TaxRate" AS tax_rate,
    "Name" AS name,
    "rowguid" AS rowguid,
    "ModifiedDate" AS modified_date

FROM {{ source('staging', 'STG_ADW_SalesTaxRate') }}
