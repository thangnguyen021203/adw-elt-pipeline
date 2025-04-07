
{{ config(materialized='view') }}

SELECT 
    "ProductSubcategoryID" AS product_subcategory_id,
    "ProductCategoryID" AS product_category_id,
    "Name" AS name,
    "rowguid" AS rowguid,
    "ModifiedDate" AS modified_date

FROM {{ source('staging', 'STG_ADW_ProductSubcategory') }}
