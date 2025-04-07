{{ config(materialized='table') }}

WITH offer AS (
    -- Lấy dữ liệu từ staging stg_special_offer
    SELECT
        special_offer_id,
        description,
        discount_pct,
        type,
        category,
        start_date,
        end_date,
        min_qty,
        max_qty,
        modified_date
    FROM {{ ref('stg_special_offer') }}
),

-- offer_product AS (
--     -- Lấy thông tin channel từ staging stg_special_offer_product
--     -- Nếu một special_offer_id có nhiều channel, ta lấy giá trị nhỏ nhất (hoặc có thể áp dụng logic khác)
--     SELECT
--         special_offer_id,
--         MIN(channel) AS channel
--     FROM {{ ref('stg_special_offer_product') }}
--     GROUP BY special_offer_id
-- ),

with_dates AS (
    SELECT
        o.special_offer_id,
        o.description,
        o.discount_pct,
        o.type,
        o.category,
        sd.datetime_key AS start_date_key,
        ed.datetime_key AS end_date_key,
        o.min_qty,
        o.max_qty
    FROM offer o
    LEFT JOIN {{ ref('dim_date') }} sd 
        ON CAST(CAST(o.start_date AS DATE) AS TIMESTAMP_NTZ) = sd.full_datetime
    LEFT JOIN {{ ref('dim_date') }} ed 
        ON CAST(CAST(o.end_date AS DATE) AS TIMESTAMP_NTZ) = ed.full_datetime
    LEFT JOIN {{ ref('dim_date') }} md 
        ON CAST(CAST(o.modified_date AS DATE) AS TIMESTAMP_NTZ) = md.full_datetime

)

SELECT
    ROW_NUMBER() OVER (ORDER BY special_offer_id) AS promotion_key,
    description,
    discount_pct,
    type,
    category,
    start_date_key,
    end_date_key,
    min_qty,
    max_qty
FROM with_dates
