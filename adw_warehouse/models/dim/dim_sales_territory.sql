{{ config(materialized='table') }}

WITH raw AS (
    SELECT
        territory_id,
        name,
        country_region_code,
        sales_territory_group,
        sales_ytd,
        sales_last_year,
        cost_ytd,
        cost_last_year
    FROM {{ ref('stg_sales_territory') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY territory_id) AS sales_territory_key,
    territory_id,
    name,
    country_region_code,
    sales_territory_group,
    sales_ytd,
    sales_last_year,
    cost_ytd,
    cost_last_year
FROM raw
