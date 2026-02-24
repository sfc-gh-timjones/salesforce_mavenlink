-- ============================================================
-- Revenue Operations dbt Project — Snowflake Prerequisites
-- ============================================================
-- Run this ENTIRE script as ACCOUNTADMIN before deploying the
-- dbt project with `snow dbt`.
--
-- What this script does:
--   1. Creates the ANALYTICS database (dbt output destination)
--   2. Creates STG / INT / MART schemas
--   3. Grants your dbt role read access to BRONZE (raw data)
--      and write access to ANALYTICS (transformed data)
--   4. Creates the External Access Integration required by
--      `snow dbt` to download dbt packages (e.g. dbt_utils)
--   5. (Optional) Creates a warehouse
--
-- Prerequisites:
--   - BRONZE.SALESFORCE and BRONZE.MAVENLINK schemas must
--     already exist with raw data (populated by Fivetran).
--
-- IMPORTANT: Replace every occurrence of <YOUR_DBT_ROLE> below
-- with the Snowflake role your dbt service user authenticates as.
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- ────────────────────────────────────────────────────────────
-- 1. Create the analytics database
-- ────────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS ANALYTICS
  COMMENT = 'dbt-managed transformed data — staging, intermediate, and mart layers';

-- ────────────────────────────────────────────────────────────
-- 2. Create the schemas that dbt writes to
-- ────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS ANALYTICS.STG
  COMMENT = 'Staging layer — 1:1 cleaned source tables';
CREATE SCHEMA IF NOT EXISTS ANALYTICS.INT
  COMMENT = 'Intermediate layer — within-domain and cross-system joins';
CREATE SCHEMA IF NOT EXISTS ANALYTICS.MART
  COMMENT = 'Mart layer — Kimball star schema (facts + dimensions)';

-- ────────────────────────────────────────────────────────────
-- 3. Grant the dbt execution role access
-- ────────────────────────────────────────────────────────────
-- Write access to ANALYTICS
GRANT USAGE ON DATABASE ANALYTICS TO ROLE <YOUR_DBT_ROLE>;
GRANT CREATE SCHEMA ON DATABASE ANALYTICS TO ROLE <YOUR_DBT_ROLE>;
GRANT ALL PRIVILEGES ON SCHEMA ANALYTICS.PUBLIC TO ROLE <YOUR_DBT_ROLE>;
GRANT ALL PRIVILEGES ON SCHEMA ANALYTICS.STG TO ROLE <YOUR_DBT_ROLE>;
GRANT ALL PRIVILEGES ON SCHEMA ANALYTICS.INT TO ROLE <YOUR_DBT_ROLE>;
GRANT ALL PRIVILEGES ON SCHEMA ANALYTICS.MART TO ROLE <YOUR_DBT_ROLE>;

-- Read access to BRONZE (raw source data)
GRANT USAGE ON DATABASE BRONZE TO ROLE <YOUR_DBT_ROLE>;
GRANT USAGE ON SCHEMA BRONZE.SALESFORCE TO ROLE <YOUR_DBT_ROLE>;
GRANT USAGE ON SCHEMA BRONZE.MAVENLINK TO ROLE <YOUR_DBT_ROLE>;
GRANT SELECT ON ALL TABLES IN SCHEMA BRONZE.SALESFORCE TO ROLE <YOUR_DBT_ROLE>;
GRANT SELECT ON ALL TABLES IN SCHEMA BRONZE.MAVENLINK TO ROLE <YOUR_DBT_ROLE>;

-- Future-proof: auto-grant on tables added later by Fivetran
GRANT SELECT ON FUTURE TABLES IN SCHEMA BRONZE.SALESFORCE TO ROLE <YOUR_DBT_ROLE>;
GRANT SELECT ON FUTURE TABLES IN SCHEMA BRONZE.MAVENLINK TO ROLE <YOUR_DBT_ROLE>;

-- ────────────────────────────────────────────────────────────
-- 4. External Access Integration for `snow dbt`
-- ────────────────────────────────────────────────────────────
-- When you run `snow dbt deploy` or `snow dbt execute`, Snowflake
-- needs to download dbt packages (like dbt_utils) from the
-- internet. This requires a Network Rule + External Access
-- Integration. Pass the integration name when deploying:
--
--   snow dbt deploy \
--     --external-access-integration DBT_EXTERNAL_ACCESS
--
--   snow dbt execute \
--     --external-access-integration DBT_EXTERNAL_ACCESS

CREATE NETWORK RULE IF NOT EXISTS ANALYTICS.PUBLIC.DBT_PACKAGES_NETWORK_RULE
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('hub.getdbt.com', 'codeload.github.com', 'github.com');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION DBT_EXTERNAL_ACCESS
  ALLOWED_NETWORK_RULES = (ANALYTICS.PUBLIC.DBT_PACKAGES_NETWORK_RULE)
  ENABLED = TRUE
  COMMENT = 'Allows snow dbt to download dbt packages (dbt_utils, etc.)';

-- ────────────────────────────────────────────────────────────
-- 5. Warehouse (use an existing one, or uncomment to create)
-- ────────────────────────────────────────────────────────────
-- CREATE WAREHOUSE IF NOT EXISTS WH_XS
--   WAREHOUSE_SIZE = 'XSMALL'
--   AUTO_SUSPEND = 60
--   AUTO_RESUME = TRUE;
-- GRANT USAGE ON WAREHOUSE WH_XS TO ROLE <YOUR_DBT_ROLE>;
