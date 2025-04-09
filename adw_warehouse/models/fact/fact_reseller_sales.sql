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
        purchase_order_number,
        account_number,
        customer_id,
        territory_id,
        currency_rate_id,
        tax_amt,
        freight,
        modified_date
    FROM {{ ref('stg_sales_order_header') }}
    WHERE online_order_flag = False  -- chỉ lấy đơn reseller/offline
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
        h.freight,
        h.modified_date
    FROM header h
    INNER JOIN detail d
      ON h.sales_order_id = d.sales_order_id
),
customer_lookup AS (
    -- Lấy thông tin StoreID từ staging Customer
    SELECT 
         customer_id,
         store_id
    FROM {{ ref('stg_customer') }}
),
with_customer AS (
    SELECT 
         j.*,
         cl.store_id
    FROM joined j
    LEFT JOIN customer_lookup cl
         ON j.customer_id = cl.customer_id
),
with_dim_keys AS (
    SELECT
         -- Tham chiếu store_key từ dim_store thông qua store_id
         ds.store_key,
         dp.product_key,
         dc.customer_key,
         dprom.promotion_key,
         dcur.currency_key,
         dship.datetime_key AS shipdate_key,
         ddue.datetime_key AS duedate_key,
         dorder.datetime_key AS orderdate_key,
         dst.sales_territory_key,
         
         with_customer.sales_order_number,
         with_customer.sales_order_detail_id AS sales_order_line_number,
         with_customer.order_qty AS order_quantity,
         with_customer.unit_price,
         with_customer.unit_price_discount AS unit_price_discount_pct,
         CAST(with_customer.unit_price * with_customer.order_qty * 1.0 AS NUMERIC(12,2)) AS extended_amount,
         with_customer.line_total AS sales_amount,
         CAST(with_customer.unit_price_discount * with_customer.unit_price * with_customer.order_qty * 1.0 AS NUMERIC(12,2)) AS discount_amount,
         with_customer.tax_amt,
         with_customer.freight,
         with_customer.revision_number,
         dp.standard_cost AS product_standard_cost,
         CAST(dp.standard_cost * with_customer.order_qty * 1.0 AS NUMERIC(12,2)) AS total_product_cost
    FROM with_customer
    LEFT JOIN {{ ref('dim_product') }} dp
         ON with_customer.product_id = dp.product_id
    LEFT JOIN {{ ref('dim_customer') }} dc
         ON with_customer.customer_id = dc.customer_id
    LEFT JOIN {{ ref('dim_promotion') }} dprom
         ON with_customer.special_offer_id = dprom.promotion_key
    LEFT JOIN {{ ref('dim_currency') }} dcur
         ON with_customer.currency_rate_id = dcur.currency_key
    LEFT JOIN {{ ref('dim_sales_territory') }} dst
         ON with_customer.territory_id = dst.territory_id
    LEFT JOIN {{ ref('dim_date') }} dship
         ON CAST(CAST(with_customer.ship_date AS DATE) AS TIMESTAMP_NTZ) = dship.full_datetime
    LEFT JOIN {{ ref('dim_date') }} ddue
         ON CAST(CAST(with_customer.due_date AS DATE) AS TIMESTAMP_NTZ) = ddue.full_datetime
    LEFT JOIN {{ ref('dim_date') }} dorder
         ON CAST(CAST(with_customer.order_date AS DATE) AS TIMESTAMP_NTZ) = dorder.full_datetime
    LEFT JOIN {{ ref('dim_store') }} ds
         ON with_customer.store_id = ds.store_id
)

SELECT * FROM with_dim_keys
