{{ config(materialized='table') }}

WITH prod AS (
    SELECT
      p.product_id,
      p.name                AS product_name,
      p.color               AS color,
      p.standard_cost        AS standard_cost,
      p.list_price           AS list_price,
      p.sell_start_date     AS launch_date,
      p.sell_end_date       AS sell_end_date,
      p.weight_unit_measure_code              AS weight,
      p.size_unit_measure_code                AS size,
      -- Lấy tên subcategory từ staging, qua join với ProductSubcategory
      ps.name               AS subcategory,
      -- Lấy tên category từ staging, qua join với ProductCategory thông qua subcategory
      pc.name               AS category,
      -- Lấy tên model từ staging
      pm.name               AS model
    FROM {{ ref('stg_product') }} p
    LEFT JOIN {{ ref('stg_product_subcategory') }} ps
           ON p.product_subcategory_id = ps.product_subcategory_id
    LEFT JOIN {{ ref('stg_product_category') }} pc
           ON ps.product_category_id   = pc.product_category_id
    LEFT JOIN {{ ref('stg_product_model') }} pm
           ON p.product_model_id       = pm.product_model_id
)

SELECT
  ROW_NUMBER() OVER (ORDER BY product_id) AS product_key,  -- Surrogate key
  product_id,         -- Business key
  product_name,
  subcategory,
  category,
  model,
  color,
  standard_cost,
  list_price,
  launch_date,
  weight,
  size
FROM prod
