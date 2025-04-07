
{{ config(materialized='view') }}

SELECT 
    "SalesOrderID" AS sales_order_id,
    "SalesOrderDetailID" AS sales_order_detail_id,
    "CarrierTrackingNumber" AS carrier_tracking_number,
    "OrderQty" AS order_qty,
    "ProductID" AS product_id,
    "SpecialOfferID" AS special_offer_id,
    "UnitPrice" AS unit_price,
    "UnitPriceDiscount" AS unit_price_discount,
    "LineTotal" AS line_total,
    "rowguid" AS rowguid,
    "ModifiedDate" AS modified_date

FROM {{ source('staging', 'STG_ADW_SalesOrderDetail') }}
