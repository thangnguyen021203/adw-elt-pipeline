
{{ config(materialized='view') }}

SELECT 
    "SpecialOfferID" AS special_offer_id,
    "Description" AS description,
    "DiscountPct" AS discount_pct,
    "Type" AS type,
    "Category" AS category,
    "StartDate" AS start_date,
    "EndDate" AS end_date,
    "MinQty" AS min_qty,
    "MaxQty" AS max_qty,
    "rowguid" AS rowguid,
    "ModifiedDate" AS modified_date
FROM {{ source('staging', 'STG_ADW_SpecialOffer') }}
