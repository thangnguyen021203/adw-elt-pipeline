
{{ config(materialized='view') }}

SELECT 
    *
FROM {{ source('staging', 'STG_ADW_Address') }}
