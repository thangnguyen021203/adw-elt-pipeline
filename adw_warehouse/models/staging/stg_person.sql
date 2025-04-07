
{{ config(materialized='view') }}

SELECT 
    "BusinessEntityID"         AS business_entity_id,
    "PersonType"               AS person_type,
    "NameStyle"                AS name_style,
    "Title"                    AS title,
    "FirstName"                AS first_name,
    "MiddleName"               AS middle_name,
    "LastName"                 AS last_name,
    "Suffix"                   AS suffix,
    "EmailPromotion"           AS email_promotion,
    "AdditionalContactInfo"    AS additional_contact_info,
    "Demographics"             AS demographics,
    "rowguid"                  AS rowguid,
    "ModifiedDate"             AS modified_date
FROM {{ source('staging', 'STG_ADW_Person') }}
