{{ config(materialized='table') }}

WITH base AS (
    SELECT 
        currency_code,
        name
    FROM {{ ref('stg_currency') }}
)

SELECT
    -- Sinh surrogate key nếu cần, ví dụ:
    ROW_NUMBER() OVER (ORDER BY currency_code) AS currency_key,
    currency_code,
    name
FROM base
