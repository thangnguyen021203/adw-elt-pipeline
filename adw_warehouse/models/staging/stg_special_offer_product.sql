
{{ config(materialized='view') }}

SELECT 
    "SpecialOfferID" AS special_offer_id,
    "ProductID" AS product_id,
    "rowguid" AS rowguid,
    "ModifiedDate" AS modified_date
FROM {{ source('staging', 'STG_ADW_SpecialOfferProduct') }}
