{{ config(materialized='table') }}

WITH union_sales AS (
    -- Hợp nhất dữ liệu từ fact_internet_sales và fact_reseller_sales
    SELECT
        customer_key,
        orderdate_key,
        sales_amount
    FROM {{ ref('fact_internet_sales') }}
    
    UNION ALL

    SELECT
        customer_key,
        orderdate_key,
        sales_amount
    FROM {{ ref('fact_reseller_sales') }}
),

sales_with_date AS (
    -- Ánh xạ orderdate_key sang ngày thực thông qua dim_date
    SELECT 
        us.customer_key,
        d.full_date AS order_date,
        us.sales_amount
    FROM union_sales us
    LEFT JOIN {{ ref('dim_date') }} d
      ON us.orderdate_key = d.datetime_key
),

aggregated AS (
    -- Tính toán các chỉ số cơ bản theo customer
    SELECT
        customer_key,
        MIN(order_date) AS first_purchase_date,
        MAX(order_date) AS last_purchase_date,
        COUNT(*) AS frequency,
        SUM(sales_amount) AS monetary
    FROM sales_with_date
    GROUP BY customer_key
),

calculated AS (
    -- Tính recency và CLV score
    SELECT
        customer_key,
        /* 
           Recency: Số ngày từ ngày mua cuối cùng đến ngày hiện tại.
           Lưu ý: CURRENT_DATE() là hàm lấy ngày hiện tại trong Snowflake.
        */
        DATEDIFF(day, last_purchase_date, CURRENT_DATE()) AS recency,
        frequency,
        monetary,
        /* CLV Score mẫu: chia tổng chi tiêu cho (recency + 1)
           Chúng ta dùng NULLIF để tránh chia cho 0, mặc dù với công thức này
           recency sẽ luôn >= 0; việc cộng 1 giúp cân bằng khi recency = 0.
        */
        monetary / NULLIF((DATEDIFF(day, last_purchase_date, CURRENT_DATE()) + 1), 0) AS clv_score,
        first_purchase_date,
        last_purchase_date
    FROM aggregated
)

SELECT
    customer_key,
    recency,
    frequency,
    monetary,
    clv_score,
    first_purchase_date,
    last_purchase_date
FROM calculated
