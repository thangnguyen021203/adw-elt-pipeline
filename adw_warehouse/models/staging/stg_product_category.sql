
{{ config(materialized='view') }}

SELECT 
    "ProductCategoryID" AS product_category_id,
    "Name" AS name,
    "rowguid" AS rowguid,
    "ModifiedDate" AS modified_date

FROM {{ source('staging', 'STG_ADW_ProductCategory') }}
