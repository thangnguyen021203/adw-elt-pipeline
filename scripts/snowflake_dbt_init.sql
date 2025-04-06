use role accountadmin;

-- if not exist michael, please create
CREATE USER michael
PASSWORD = 'asd123'
LOGIN_NAME = 'michael'
DEFAULT_SECONDARY_ROLES = ('ALL');

-- use only on test
drop warehouse if exists dbt_wh;

create warehouse dbt_wh with warehouse_size='x-small';
create database if not exists dbt_db;
create role if not exists dbt_role;

show grants on warehouse dbt_wh;

grant usage on warehouse dbt_wh to role dbt_role;
grant role dbt_role to user michael;
grant all on database dbt_db to role dbt_role;

GRANT OWNERSHIP ON DATABASE dbt_db TO ROLE dbt_role REVOKE CURRENT GRANTS;
show grants on warehouse dbt_wh;

SELECT CURRENT_USER();
GRANT ROLE dbt_role TO USER thanhbuik306;

use role dbt_role;
-- create schema dbt_db.staging;
use role accountadmin;
CREATE OR REPLACE STAGE dbt_db.staging.staging_stage;
use role accountadmin;
GRANT USAGE ON SCHEMA dbt_db.staging TO ROLE dbt_role;
GRANT READ, WRITE ON STAGE dbt_db.staging.staging_stage TO ROLE dbt_role;


LIST @dbt_db.staging.staging_stage;
SHOW TABLES IN SCHEMA dbt_db.staging;

CREATE OR REPLACE FILE FORMAT staging_parquet_format
  TYPE = PARQUET
  USE_LOGICAL_TYPE = TRUE;

use role dbt_role;
SELECT * FROM dbt_db.staging."STG_ADW_SalesTaxRate" LIMIT 10;
SELECT * FROM dbt_db.staging."STG_ADW_Store" LIMIT 10;
DESC TABLE dbt_db.staging."STG_ADW_SalesTaxRate";
SELECT * FROM dbt_db.staging."STG_ADW_SpecialOffer" LIMIT 10;


REMOVE @dbt_db.staging.staging_stage PATTERN='.*\.parquet.*';

SHOW STAGES IN SCHEMA dbt_db.staging;

DESC TABLE dbt_db.staging."STG_ADW_SpecialOffer";
