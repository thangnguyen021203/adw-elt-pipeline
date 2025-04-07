{{ config(materialized='table') }}

WITH store_raw AS (
    SELECT
        business_entity_id AS store_id,
        name AS store_name,
        salesperson_id,
        -- Parse XML thành VARIANT (không dùng OBJECT!)
        TRY_CAST(PARSE_XML(demographics) AS OBJECT) AS survey_xml
    FROM {{ ref('stg_store') }}
),


salesperson_info AS (
    SELECT
      business_entity_id AS salesperson_id,
      territory_id
    FROM {{ ref('stg_sales_person') }}
),

store_with_salesperson AS (
    SELECT
      sr.*,
      sp.territory_id
    FROM store_raw sr
    LEFT JOIN salesperson_info sp
      ON sr.salesperson_id = sp.salesperson_id
),

store_address AS (
    SELECT
        bea.business_entity_id AS store_id,
        a.state_province_id
    FROM {{ ref('stg_business_entity_address') }} bea
    JOIN {{ ref('stg_address') }} a
      ON bea.address_id = a.address_id
),

store_raw_with_state AS (
    SELECT
        sws.*,
        sa.state_province_id
    FROM store_with_salesperson sws
    LEFT JOIN store_address sa
      ON sws.store_id = sa.store_id
),

store_demographics AS (
    SELECT
        store_id,
        store_name,
        territory_id,
        state_province_id,
        salesperson_id,

        TRY_TO_NUMBER(survey_xml:"AnnualSales"::VARCHAR) AS annual_sales,
        TRY_TO_NUMBER(survey_xml:"AnnualRevenue"::VARCHAR) AS annual_revenue,
        TRY_TO_NUMBER(survey_xml:"SquareFeet"::VARCHAR) AS square_feet,
        TRY_TO_NUMBER(survey_xml:"Brands"::VARCHAR) AS brands,
        TRY_TO_NUMBER(survey_xml:"NumberEmployees"::VARCHAR) AS number_employees,

        survey_xml:"Internet"::VARCHAR AS internet,
        survey_xml:"BankName"::VARCHAR AS bank_name,
        survey_xml:"BusinessType"::VARCHAR AS business_type,
        survey_xml:"YearOpened"::VARCHAR AS year_opened_str,
        survey_xml:"Specialty"::VARCHAR AS specialty

    FROM store_raw_with_state
),



store_with_territory AS (
    SELECT
      d.*,
      t.name AS territory_name,
      t.country_region_code,
      t.sales_territory_group AS region,
      t.sales_ytd,
      t.sales_last_year,
      t.cost_ytd,
      t.cost_last_year
    FROM store_demographics d
    LEFT JOIN {{ ref('stg_sales_territory') }} t
      ON d.territory_id = t.territory_id
),

store_final AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY store_id) AS store_key,
        store_id,
        store_name,
        territory_name AS territory,
        region,
        number_employees AS employee_count,
        square_feet AS store_size,
        business_type AS store_type,
        annual_sales,
        annual_revenue,
        bank_name,
        specialty,
        year_opened_str,
        state_province_id
    FROM store_with_territory
)

SELECT * FROM store_final