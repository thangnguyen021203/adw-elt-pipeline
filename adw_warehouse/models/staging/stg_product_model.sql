
{{ config(materialized='view') }}

SELECT 
    "ProductModelID" AS product_model_id,
    "Name" AS name,
    "CatalogDescription" AS catalog_description,
    "Instructions" AS instructions,
    "rowguid" AS rowguid,
    "ModifiedDate" AS modified_date

FROM {{ source('staging', 'STG_ADW_ProductModel') }}
