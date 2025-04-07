{{ config(materialized='table') }}

-- 1. Lấy thông tin khách hàng cơ bản từ staging Person
WITH raw_customers AS (
    SELECT
        p.business_entity_id AS customer_id,
        p.first_name,
        p.last_name,
        ea.email_address,
        PARSE_XML(p.demographics) AS survey_xml
    FROM {{ ref('stg_person') }} p
    LEFT JOIN {{ ref('stg_email_address') }} ea
        ON p.business_entity_id = ea.business_entity_id
),

-- 2. Trích xuất các trường demographic từ XML
customer_demographics AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        email_address,
        GET(XMLGET(survey_xml, 'Gender'), '$')::STRING                     AS gender,
        TRY_TO_DATE(GET(XMLGET(survey_xml, 'BirthDate'), '$')::STRING)    AS birth_date,
        GET(XMLGET(survey_xml, 'MaritalStatus'), '$')::STRING             AS marital_status,
        GET(XMLGET(survey_xml, 'YearlyIncome'), '$')::STRING              AS yearly_income,
        GET(XMLGET(survey_xml, 'Education'), '$')::STRING                 AS education_level,
        GET(XMLGET(survey_xml, 'HomeOwnerFlag'), '$')::STRING::INT        AS home_owner_flag,
        GET(XMLGET(survey_xml, 'CommuteDistance'), '$')::STRING           AS commute_distance,
        GET(XMLGET(survey_xml, 'NumberChildrenAtHome'), '$')::STRING::INT AS number_children,
        GET(XMLGET(survey_xml, 'TotalChildren'), '$')::STRING::INT        AS total_children,
        GET(XMLGET(survey_xml, 'NumberCarsOwned'), '$')::STRING::INT      AS number_cars_owned,
        GET(XMLGET(survey_xml, 'TotalPurchaseYTD'), '$')::STRING::FLOAT   AS total_purchase_ytd,
        TRY_TO_DATE(GET(XMLGET(survey_xml, 'DateFirstPurchase'), '$')::STRING) AS first_purchase_date,
        GET(XMLGET(survey_xml, 'Occupation'), '$')::STRING                AS occupation

    FROM raw_customers
),


-- 3. Lấy thông tin địa lý từ các bảng liên kết:
--    - Person liên kết với BusinessEntityAddress để lấy address_id
--    - Từ Address lấy được City
--    - Từ Address join với StateProvince để lấy tên tỉnh (province)
--    - Từ StateProvince join với CountryRegion để lấy mã Country (country)
customer_address AS (
    SELECT
        p.business_entity_id AS customer_id,
        a.city AS city,
        sp.name AS province,
        cr.country_region_code AS country
    FROM {{ ref('stg_person') }} p
    LEFT JOIN {{ ref('stg_business_entity_address') }} bea
        ON p.business_entity_id = bea.business_entity_id
    LEFT JOIN {{ ref('stg_address') }} a
        ON bea.address_id = a.address_id
    LEFT JOIN {{ ref('stg_state_province') }} sp
        ON a.state_province_id = sp.state_province_id
    LEFT JOIN {{ ref('stg_country_region') }} cr
        ON sp.country_region_code = cr.country_region_code
),

-- 4. Xây dựng bảng cuối cùng dim_customer với các xử lý cần thiết
final_customer AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY cd.customer_id) AS customer_key,
        cd.customer_id,
        cd.first_name || ' ' || cd.last_name AS full_name,
        cd.email_address AS email,

        -- Gender chuẩn hóa
        CASE 
            WHEN LOWER(cd.gender) IN ('m', 'male') THEN 'Male'
            WHEN LOWER(cd.gender) IN ('f', 'female') THEN 'Female'
            ELSE cd.gender
        END AS gender,

        -- Tính tuổi (birth_date giờ đã là DATE)
        CASE 
            WHEN cd.birth_date IS NOT NULL THEN 
                DATE_PART(year, CURRENT_DATE) - DATE_PART(year, cd.birth_date)
            ELSE NULL 
        END AS age,

        -- Parse income dạng '25001-50000' thành trung bình
        CASE 
            WHEN cd.yearly_income ILIKE '%-%' THEN 
                (TRY_TO_NUMBER(SPLIT_PART(cd.yearly_income, '-', 1)) + 
                 TRY_TO_NUMBER(SPLIT_PART(cd.yearly_income, '-', 2))) / 2
            ELSE TRY_TO_NUMBER(cd.yearly_income)
        END AS income,

        cd.education_level,
        cd.marital_status,

        -- Home owner flag: 1 → true
        CASE WHEN cd.home_owner_flag = 1 THEN TRUE ELSE FALSE END AS home_owner,

        cd.commute_distance,

        -- Ưu tiên number_children, fallback total_children
        COALESCE(cd.number_children, cd.total_children) AS number_of_children,

        -- Customer type dựa theo store/person
        CASE 
            WHEN c.store_id IS NOT NULL THEN 'Store'
            WHEN c.person_id IS NOT NULL THEN 'Individual'
            ELSE 'Unknown'
        END AS customer_type,

        -- Thông tin địa lý
        ca.country,
        ca.city,
        ca.province
    FROM customer_demographics cd
    LEFT JOIN customer_address ca ON cd.customer_id = ca.customer_id
    LEFT JOIN {{ ref('stg_customer') }} c ON cd.customer_id = c.person_id
)


SELECT * FROM final_customer
