{{ config(materialized='table') }}

WITH store_raw AS (
    SELECT
        business_entity_id AS store_id,
        name AS store_name,
        salesperson_id,
        PARSE_XML(demographics) AS survey_xml
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

        GET(XMLGET(survey_xml, 'AnnualSales'), '$')::STRING::NUMBER       AS annual_sales,
        GET(XMLGET(survey_xml, 'AnnualRevenue'), '$')::STRING::NUMBER     AS annual_revenue,
        GET(XMLGET(survey_xml, 'SquareFeet'), '$')::STRING::NUMBER        AS square_feet,
        GET(XMLGET(survey_xml, 'Brands'), '$')::STRING                    AS brands,
        GET(XMLGET(survey_xml, 'NumberEmployees'), '$')::STRING::NUMBER   AS number_employees,

        XMLGET(XMLGET(survey_xml, 'Internet'), '$')::STRING                  AS internet,
        GET(XMLGET(survey_xml, 'BankName'), '$')::STRING                  AS bank_name,
        GET(XMLGET(survey_xml, 'BusinessType'), '$')::STRING              AS business_type,
        GET(XMLGET(survey_xml, 'YearOpened'), '$')::STRING::NUMBER        AS year_opened,
        GET(XMLGET(survey_xml, 'Specialty'), '$')::STRING                 AS specialty

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
        year_opened,
        state_province_id
    FROM store_with_territory
)

SELECT * FROM store_final