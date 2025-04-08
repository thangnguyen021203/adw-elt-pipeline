{{ config(materialized='table') }}

WITH reseller AS (
    -- Lấy dữ liệu bán hàng theo kênh reseller đã có store_key (đã join với dim_store trong fact_reseller_sales)
    SELECT
        sales_order_number,
        store_key,
        orderdate_key,
        customer_key,
        sales_amount
    FROM {{ ref('fact_reseller_sales') }}
),

daily_aggregates AS (
    SELECT
        store_key,
        orderdate_key AS date_key,
        SUM(sales_amount) AS total_sales,
        COUNT(DISTINCT sales_order_number) AS order_count,
        COUNT(DISTINCT customer_key) AS customer_count
    FROM reseller
    GROUP BY store_key, orderdate_key
),

store_performance AS (
    SELECT
        store_key,
        date_key,
        total_sales,
        order_count,
        customer_count,
        CASE 
          WHEN order_count = 0 THEN 0 
          ELSE total_sales / order_count 
        END AS avg_income_per_order
    FROM daily_aggregates
)

SELECT
    store_key,
    date_key,
    total_sales,
    order_count,
    customer_count,
    avg_income_per_order
FROM store_performance
