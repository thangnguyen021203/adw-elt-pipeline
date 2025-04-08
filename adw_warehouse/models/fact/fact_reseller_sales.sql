{{ config(materialized='table') }}

WITH header AS (
    SELECT
        sales_order_id,
        revision_number,
        order_date,
        due_date,
        ship_date,
        online_order_flag,
        sales_order_number,
        customer_id,
        territory_id,
        currency_rate_id,
        tax_amt,
        freight
    FROM {{ ref('stg_sales_order_header') }}
    WHERE online_order_flag = False
),

detail AS (
    SELECT
        sales_order_id,
        sales_order_detail_id,
        order_qty,
        product_id,
        special_offer_id,
        unit_price,
        unit_price_discount,
        line_total,
        modified_date
    FROM {{ ref('stg_sales_order_detail') }}
),

joined AS (
    SELECT
        h.sales_order_id,
        d.sales_order_detail_id,
        h.revision_number,
        h.sales_order_number,
        d.order_qty,
        d.unit_price,
        d.unit_price_discount,
        d.line_total,
        d.product_id,
        h.customer_id,
        h.territory_id,
        d.special_offer_id,
        h.currency_rate_id,
        h.order_date,
        h.due_date,
        h.ship_date,
        h.tax_amt,
        h.freight
    FROM header h
    INNER JOIN detail d
        ON h.sales_order_id = d.sales_order_id
),
with_dim_keys AS (
    SELECT
        -- Surrogate keys
        dp.product_key,
        dc.customer_key,
        dprom.promotion_key,
        dcur.currency_key,
        dship.datetime_key AS shipdate_key,
        ddue.datetime_key AS duedate_key,
        dorder.datetime_key AS orderdate_key,
        dst.sales_territory_key,

        -- Business identifiers
        j.sales_order_number,
        j.sales_order_detail_id AS sales_order_line_number,

        -- Metrics
        j.order_qty AS order_quantity,
        j.unit_price,
        j.unit_price_discount AS unit_price_discount_pct,
        CAST(unit_price * order_qty * 1.0 AS NUMERIC(12,2)) AS extended_amount,
        j.line_total AS sales_amount,
        CAST(unit_price_discount * unit_price * order_qty * 1.0 AS NUMERIC(12,2)) AS discount_amount,
        j.tax_amt,
        j.freight,
        j.revision_number,
        dp.standard_cost AS product_standard_cost,
        CAST(dp.standard_cost * j.order_qty * 1.0 AS NUMERIC(12,2)) AS total_product_cost

    FROM joined j

    LEFT JOIN {{ ref('dim_product') }} dp
        ON j.product_id = dp.product_id

    LEFT JOIN {{ ref('dim_customer') }} dc
        ON j.customer_id = dc.customer_id

    LEFT JOIN {{ ref('dim_promotion') }} dprom
        ON j.special_offer_id = dprom.promotion_key  -- hoặc business key nếu cần map lại

    LEFT JOIN {{ ref('dim_currency') }} dcur
        ON j.currency_rate_id = dcur.currency_key  -- nếu currency key là rate_id

    LEFT JOIN {{ ref('dim_sales_territory') }} dst
        ON j.territory_id = dst.territory_id

    LEFT JOIN {{ ref('dim_date') }} dship
        ON CAST(CAST(j.ship_date AS DATE) AS TIMESTAMP_NTZ) = dship.full_datetime

    LEFT JOIN {{ ref('dim_date') }} ddue
        ON CAST(CAST(j.due_date AS DATE) AS TIMESTAMP_NTZ) = ddue.full_datetime

    LEFT JOIN {{ ref('dim_date') }} dorder
        ON CAST(CAST(j.order_date AS DATE) AS TIMESTAMP_NTZ) = dorder.full_datetime
)

SELECT * FROM with_dim_keys
