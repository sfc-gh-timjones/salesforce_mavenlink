-- ============================================================
-- COCO dbt Project — Snowflake Prerequisites
-- ============================================================
-- Run this script as ACCOUNTADMIN (or a role with CREATE DATABASE)
-- before executing the dbt project.
--
-- This creates the ANALYTICS database where dbt writes its
-- transformed tables (STG, INT, MART schemas). dbt will create
-- the schemas automatically on first run.
--
-- Your raw source data (BRONZE.SALESFORCE, BRONZE.MAVENLINK)
-- must already exist — those are populated by Fivetran.
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- 1. Create the analytics database
CREATE DATABASE IF NOT EXISTS ANALYTICS
  COMMENT = 'dbt-managed transformed data — staging, intermediate, and mart layers';

-- 2. Create the schemas that dbt writes to
CREATE SCHEMA IF NOT EXISTS ANALYTICS.STG
  COMMENT = 'Staging layer — 1:1 cleaned source tables';
CREATE SCHEMA IF NOT EXISTS ANALYTICS.INT
  COMMENT = 'Intermediate layer — within-domain and cross-system joins';
CREATE SCHEMA IF NOT EXISTS ANALYTICS.MART
  COMMENT = 'Mart layer — Kimball star schema (facts + dimensions)';

-- 3. Grant the dbt execution role access
-- Replace <YOUR_DBT_ROLE> with whatever role your dbt service user has.
GRANT USAGE ON DATABASE ANALYTICS TO ROLE <YOUR_DBT_ROLE>;
GRANT CREATE SCHEMA ON DATABASE ANALYTICS TO ROLE <YOUR_DBT_ROLE>;
GRANT ALL PRIVILEGES ON SCHEMA ANALYTICS.PUBLIC TO ROLE <YOUR_DBT_ROLE>;
GRANT ALL PRIVILEGES ON SCHEMA ANALYTICS.STG TO ROLE <YOUR_DBT_ROLE>;
GRANT ALL PRIVILEGES ON SCHEMA ANALYTICS.INT TO ROLE <YOUR_DBT_ROLE>;
GRANT ALL PRIVILEGES ON SCHEMA ANALYTICS.MART TO ROLE <YOUR_DBT_ROLE>;

-- 4. dbt also needs to READ from the raw source database
GRANT USAGE ON DATABASE BRONZE TO ROLE <YOUR_DBT_ROLE>;
GRANT USAGE ON SCHEMA BRONZE.SALESFORCE TO ROLE <YOUR_DBT_ROLE>;
GRANT USAGE ON SCHEMA BRONZE.MAVENLINK TO ROLE <YOUR_DBT_ROLE>;
GRANT SELECT ON ALL TABLES IN SCHEMA BRONZE.SALESFORCE TO ROLE <YOUR_DBT_ROLE>;
GRANT SELECT ON ALL TABLES IN SCHEMA BRONZE.MAVENLINK TO ROLE <YOUR_DBT_ROLE>;

-- If new tables are added to BRONZE by Fivetran in the future:
GRANT SELECT ON FUTURE TABLES IN SCHEMA BRONZE.SALESFORCE TO ROLE <YOUR_DBT_ROLE>;
GRANT SELECT ON FUTURE TABLES IN SCHEMA BRONZE.MAVENLINK TO ROLE <YOUR_DBT_ROLE>;

-- 5. Warehouse (use an existing warehouse or create one)
-- CREATE WAREHOUSE IF NOT EXISTS WH_XS
--   WAREHOUSE_SIZE = 'XSMALL'
--   AUTO_SUSPEND = 60
--   AUTO_RESUME = TRUE;
-- GRANT USAGE ON WAREHOUSE WH_XS TO ROLE <YOUR_DBT_ROLE>;
