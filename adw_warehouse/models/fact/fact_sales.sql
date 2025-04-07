{{ config(materialized='table') }}

WITH sales_header AS (
    -- Lấy dữ liệu header của đơn hàng
    SELECT
       sales_order_id        AS sales_order_id,
       OrderDate,
       CustomerID,
       StoreID,
       PromotionID,         -- có thể NULL nếu đơn hàng không áp dụng khuyến mãi
       Channel              -- nếu có cột channel từ staging
    FROM {{ ref('stg_sales_order_header') }}
),

sales_detail AS (
    -- Lấy dữ liệu chi tiết đơn hàng
    SELECT
       sales_order_id,
       product_id,
       order_qty            AS order_quantity,
       unit_price,
       DiscountAmount      AS discount_amount,
       LineTotal           AS sales_amount,  -- hoặc tính lại nếu cần: (unit_price * order_qty - DiscountAmount)
       ReturnFlag          AS return_flag
    FROM {{ ref('stg_sales_order_detail') }}
),

combined_sales AS (
    -- Kết hợp dữ liệu header và detail dựa trên sales_order_id
    SELECT
       h.sales_order_id,
       h.OrderDate,
       h.CustomerID,
       h.StoreID,
       h.PromotionID,
       h.Channel,
       d.product_id,
       d.order_quantity,
       d.unit_price,
       d.discount_amount,
       d.sales_amount,
       d.return_flag
    FROM sales_header h
    JOIN sales_detail d
      ON h.sales_order_id = d.sales_order_id
),

mapped_sales AS (
    -- Ánh xạ sang dimension key từ các bảng dim đã tạo
    SELECT
       cs.sales_order_id,
       d.date_key,
       p.product_key,
       c.customer_key,
       s.store_key,
       pr.promotion_key,
       cs.order_quantity,
       cs.unit_price AS unit_price,
       cs.discount_amount,
       cs.sales_amount,
       cs.return_flag,
       cs.Channel AS channel
    FROM combined_sales cs
    LEFT JOIN {{ ref('dim_date') }} d
      ON TO_DATE(cs.OrderDate) = d.full_date
    LEFT JOIN {{ ref('dim_product') }} p
      ON cs.product_id = p.product_id
    LEFT JOIN {{ ref('dim_customer') }} c
      ON cs.CustomerID = c.customer_id
    LEFT JOIN {{ ref('dim_store') }} s
      ON cs.StoreID = s.store_id
    LEFT JOIN {{ ref('dim_promotion') }} pr
      ON cs.PromotionID = pr.SpecialOfferID  -- giả sử cột này lưu giá trị từ Promotion
)

SELECT
    sales_order_id,
    date_key,
    product_key,
    customer_key,
    store_key,
    promotion_key,
    order_quantity,
    unit_price,
    discount_amount,
    sales_amount,
    return_flag,
    channel
FROM mapped_sales;
