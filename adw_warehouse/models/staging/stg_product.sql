
{{ config(materialized='view') }}

SELECT 
    "ProductID" AS product_id,
    "Name" AS name,
    "ProductNumber" AS product_number,
    "MakeFlag" AS make_flag,
    "FinishedGoodsFlag" AS finished_goods_flag,
    "Color" AS color,
    "SafetyStockLevel" AS safety_stock_level,
    "ReorderPoint" AS reorder_point,
    "StandardCost" AS standard_cost,
    "ListPrice" AS list_price,
    "Size" AS size,
    "SizeUnitMeasureCode" AS size_unit_measure_code,
    "WeightUnitMeasureCode" AS weight_unit_measure_code,
    "Weight" AS weight,
    "DaysToManufacture" AS days_to_manufacture,
    "ProductLine" AS product_line,
    "Class" AS class,
    "Style" AS style,
    "ProductSubcategoryID" AS product_subcategory_id,
    "ProductModelID" AS product_model_id,
    "SellStartDate" AS sell_start_date,
    "SellEndDate" AS sell_end_date,
    "DiscontinuedDate" AS discontinued_date,
    "rowguid" AS rowguid,
    "ModifiedDate" AS modified_date

FROM {{ source('staging', 'STG_ADW_Product') }}
