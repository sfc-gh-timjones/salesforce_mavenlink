/*
 * Revenue Operations - Semantic Views Deployment Script
 * 
 * Creates 8 semantic views for A/B testing 3 different agent configurations.
 * Includes all 10 tables (5 dimensions + 5 facts) with proper relationships.
 *
 * Fixes applied:
 *   1. Unified view includes all 9 tables (4 dimensions + 5 facts, DIM_DATE removed)
 *   2. WORKSPACE_ID typed as NUMBER (not VARCHAR)
 *   3. HEALTH_STATUS filter uses LIKE 'At Risk%' (not IN ('OVER_BUDGET', 'AT_RISK'))
 *   4. DIM_USER included with utilization metrics
 *   5. Verified queries (VQRs) added for common questions
 *   6. Uses actual column names from tables
 *   7. Added opportunity_to_rep relationship (OWNER_ID → SF_USER_ID)
 *   8. Added time_entry_to_user relationship (USER_ID → ML_USER_ID)
 *
 * Prerequisites:
 *   - ANALYTICS.MART schema with dbt-materialized tables
 *   - CREATE SEMANTIC VIEW privilege
 *
 * Usage: Run in Snowsight or snowsql -c <connection> -f deploy.sql
 */

USE ROLE ACCOUNTADMIN;
USE DATABASE ANALYTICS;
USE SCHEMA MART;
USE WAREHOUSE WH_XS;

-- ============================================================================
-- AGENT 1: UNIFIED SEMANTIC VIEW (All 10 tables in one view)
-- ============================================================================

CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML('ANALYTICS.MART', $$
name: REVENUE_OPERATIONS_UNIFIED
description: >
  Comprehensive unified semantic view for Revenue Operations analytics combining 
  Salesforce CRM and Mavenlink project data. 9 tables (4 dimensions + 5 facts)
  with full relationships for cross-system analysis.

tables:
  - name: customers
    description: Customer/Account dimension from Salesforce
    base_table:
      database: ANALYTICS
      schema: MART
      table: DIM_CUSTOMER
    dimensions:
      - name: account_id
        expr: ACCOUNT_ID
        data_type: VARCHAR
        unique: true
      - name: customer_name
        expr: ACCOUNT_NAME
        data_type: VARCHAR
        synonyms: [account_name, company_name, client_name]
      - name: industry
        expr: INDUSTRY
        data_type: VARCHAR
      - name: billing_state
        expr: BILLING_STATE
        data_type: VARCHAR
      - name: billing_country
        expr: BILLING_COUNTRY
        data_type: VARCHAR
    facts:
      - name: total_won_revenue
        expr: TOTAL_WON_REVENUE
        data_type: NUMBER
      - name: open_opportunities
        expr: OPEN_OPPORTUNITIES
        data_type: NUMBER
      - name: open_pipeline_value
        expr: OPEN_PIPELINE_VALUE
        data_type: NUMBER
      - name: win_rate_pct
        expr: WIN_RATE_PCT
        data_type: NUMBER
    metrics:
      - name: customer_revenue
        expr: SUM(TOTAL_WON_REVENUE)
      - name: customer_count
        expr: COUNT(DISTINCT ACCOUNT_ID)

  - name: users
    description: User dimension combining Salesforce and Mavenlink users
    base_table:
      database: ANALYTICS
      schema: MART
      table: DIM_USER
    dimensions:
      - name: user_key
        expr: USER_KEY
        data_type: VARCHAR
        unique: true
      - name: sf_user_id
        expr: SF_USER_ID
        data_type: VARCHAR
        unique: true
      - name: ml_user_id
        expr: ML_USER_ID
        data_type: NUMBER
        unique: true
      - name: full_name
        expr: FULL_NAME
        data_type: VARCHAR
        synonyms: [user_name, consultant_name, rep_name]
      - name: email
        expr: EMAIL
        data_type: VARCHAR
      - name: title
        expr: TITLE
        data_type: VARCHAR
      - name: department
        expr: DEPARTMENT
        data_type: VARCHAR
      - name: match_status
        expr: MATCH_STATUS
        data_type: VARCHAR
    facts:
      - name: ml_total_hours
        expr: ML_TOTAL_HOURS
        data_type: NUMBER
        synonyms: [total_hours]
      - name: ml_billable_hours
        expr: ML_BILLABLE_HOURS
        data_type: NUMBER
        synonyms: [billable_hours]
      - name: ml_utilization_pct
        expr: ML_UTILIZATION_PCT
        data_type: NUMBER
        synonyms: [utilization, utilization_rate]
      - name: ml_active_projects
        expr: ML_ACTIVE_PROJECTS
        data_type: NUMBER
    metrics:
      - name: avg_utilization
        expr: AVG(ML_UTILIZATION_PCT)
      - name: total_consultant_hours
        expr: SUM(ML_TOTAL_HOURS)
      - name: user_count
        expr: COUNT(DISTINCT USER_KEY)

  - name: products
    description: Product dimension from Salesforce
    base_table:
      database: ANALYTICS
      schema: MART
      table: DIM_PRODUCT
    dimensions:
      - name: product_id
        expr: PRODUCT_ID
        data_type: VARCHAR
        unique: true
      - name: product_name
        expr: PRODUCT_NAME
        data_type: VARCHAR
      - name: product_code
        expr: PRODUCT_CODE
        data_type: VARCHAR
      - name: product_family
        expr: PRODUCT_FAMILY
        data_type: VARCHAR
        synonyms: [category, family]
      - name: is_active
        expr: IS_ACTIVE
        data_type: BOOLEAN
    metrics:
      - name: product_count
        expr: COUNT(DISTINCT PRODUCT_ID)

  - name: projects
    description: Project dimension from Mavenlink
    base_table:
      database: ANALYTICS
      schema: MART
      table: DIM_PROJECT
    dimensions:
      - name: workspace_id
        expr: WORKSPACE_ID
        data_type: NUMBER
        unique: true
        synonyms: [project_id]
      - name: project_name
        expr: PROJECT_NAME
        data_type: VARCHAR
        synonyms: [workspace_name]
      - name: project_status
        expr: PROJECT_STATUS
        data_type: VARCHAR
        synonyms: [status]
      - name: is_budgeted
        expr: IS_BUDGETED
        data_type: BOOLEAN
      - name: is_overdue
        expr: IS_OVERDUE
        data_type: BOOLEAN
      - name: linked_opportunity_id
        expr: LINKED_OPPORTUNITY_ID
        data_type: VARCHAR
      - name: linked_account_id
        expr: LINKED_ACCOUNT_ID
        data_type: VARCHAR
    time_dimensions:
      - name: start_date
        expr: START_DATE
        data_type: DATE
      - name: due_date
        expr: DUE_DATE
        data_type: DATE
    facts:
      - name: budget_dollars
        expr: BUDGET_DOLLARS
        data_type: NUMBER
        synonyms: [budget]
      - name: budget_used_dollars
        expr: BUDGET_USED_DOLLARS
        data_type: NUMBER
      - name: percentage_complete
        expr: PERCENTAGE_COMPLETE
        data_type: NUMBER
      - name: total_hours
        expr: TOTAL_HOURS
        data_type: NUMBER
      - name: billable_hours
        expr: BILLABLE_HOURS
        data_type: NUMBER
      - name: total_revenue
        expr: TOTAL_BILLABLE_REVENUE
        data_type: NUMBER
      - name: total_cost
        expr: TOTAL_COST
        data_type: NUMBER
    metrics:
      - name: project_count
        expr: COUNT(DISTINCT WORKSPACE_ID)
      - name: total_budget
        expr: SUM(BUDGET_DOLLARS)
      - name: avg_completion
        expr: AVG(PERCENTAGE_COMPLETE)

  - name: opportunities
    description: Sales opportunity fact table from Salesforce
    base_table:
      database: ANALYTICS
      schema: MART
      table: FACT_OPPORTUNITY
    dimensions:
      - name: opportunity_id
        expr: OPPORTUNITY_ID
        data_type: VARCHAR
        unique: true
        synonyms: [deal_id]
      - name: account_id
        expr: ACCOUNT_ID
        data_type: VARCHAR
      - name: owner_id
        expr: OWNER_ID
        data_type: VARCHAR
      - name: opportunity_name
        expr: OPPORTUNITY_NAME
        data_type: VARCHAR
        synonyms: [deal_name]
      - name: account_name
        expr: ACCOUNT_NAME
        data_type: VARCHAR
      - name: stage_name
        expr: STAGE_NAME
        data_type: VARCHAR
        synonyms: [stage, sales_stage]
      - name: deal_status
        expr: DEAL_STATUS
        data_type: VARCHAR
      - name: is_won
        expr: IS_WON
        data_type: BOOLEAN
      - name: is_closed
        expr: IS_CLOSED
        data_type: BOOLEAN
      - name: owner_name
        expr: OWNER_NAME
        data_type: VARCHAR
        synonyms: [sales_rep, rep_name]
      - name: opportunity_type
        expr: OPPORTUNITY_TYPE
        data_type: VARCHAR
        synonyms: [deal_type]
      - name: forecast_category
        expr: FORECAST_CATEGORY
        data_type: VARCHAR
      - name: has_linked_project
        expr: HAS_LINKED_PROJECT
        data_type: BOOLEAN
    time_dimensions:
      - name: close_date
        expr: CLOSE_DATE
        data_type: DATE
    facts:
      - name: amount
        expr: AMOUNT
        data_type: NUMBER
        synonyms: [deal_value, revenue, deal_amount]
      - name: probability
        expr: PROBABILITY
        data_type: NUMBER
      - name: days_to_close
        expr: DAYS_TO_CLOSE
        data_type: NUMBER
    metrics:
      - name: total_pipeline
        expr: SUM(AMOUNT)
      - name: total_won
        expr: SUM(CASE WHEN IS_WON THEN AMOUNT ELSE 0 END)
      - name: opportunity_count
        expr: COUNT(*)
      - name: win_rate
        expr: ROUND(100.0 * SUM(CASE WHEN IS_WON THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN IS_CLOSED THEN 1 ELSE 0 END), 0), 2)
      - name: avg_deal_size
        expr: AVG(AMOUNT)
    filters:
      - name: open_pipeline
        expr: "IS_CLOSED = FALSE"
      - name: won_deals
        expr: "IS_WON = TRUE"

  - name: line_items
    description: Opportunity line item fact table
    base_table:
      database: ANALYTICS
      schema: MART
      table: FACT_OPPORTUNITY_LINE_ITEM
    dimensions:
      - name: line_item_id
        expr: OPPORTUNITY_LINE_ITEM_ID
        data_type: VARCHAR
        unique: true
      - name: opportunity_id
        expr: OPPORTUNITY_ID
        data_type: VARCHAR
      - name: account_id
        expr: ACCOUNT_ID
        data_type: VARCHAR
      - name: product_id
        expr: PRODUCT_ID
        data_type: VARCHAR
      - name: product_name
        expr: PRODUCT_NAME
        data_type: VARCHAR
      - name: product_family
        expr: PRODUCT_FAMILY
        data_type: VARCHAR
        synonyms: [category]
      - name: opportunity_stage
        expr: OPPORTUNITY_STAGE
        data_type: VARCHAR
      - name: opportunity_is_won
        expr: OPPORTUNITY_IS_WON
        data_type: BOOLEAN
    time_dimensions:
      - name: close_date
        expr: OPPORTUNITY_CLOSE_DATE
        data_type: DATE
      - name: service_date
        expr: SERVICE_DATE
        data_type: DATE
    facts:
      - name: quantity
        expr: QUANTITY
        data_type: NUMBER
      - name: unit_price
        expr: UNIT_PRICE
        data_type: NUMBER
      - name: total_price
        expr: TOTAL_PRICE
        data_type: NUMBER
        synonyms: [line_revenue]
      - name: discount_percent
        expr: DISCOUNT_PERCENT
        data_type: NUMBER
    metrics:
      - name: total_line_revenue
        expr: SUM(TOTAL_PRICE)
      - name: total_quantity
        expr: SUM(QUANTITY)
      - name: avg_discount
        expr: AVG(DISCOUNT_PERCENT)
      - name: line_item_count
        expr: COUNT(*)

  - name: time_entries
    description: Time entry fact table from Mavenlink
    base_table:
      database: ANALYTICS
      schema: MART
      table: FACT_TIME_ENTRY
    dimensions:
      - name: time_entry_id
        expr: TIME_ENTRY_ID
        data_type: NUMBER
        unique: true
      - name: workspace_id
        expr: WORKSPACE_ID
        data_type: NUMBER
        synonyms: [project_id]
      - name: user_id
        expr: USER_ID
        data_type: NUMBER
      - name: user_name
        expr: USER_NAME
        data_type: VARCHAR
        synonyms: [consultant_name, resource_name]
      - name: project_title
        expr: PROJECT_TITLE
        data_type: VARCHAR
        synonyms: [project_name]
      - name: project_status
        expr: PROJECT_STATUS
        data_type: VARCHAR
      - name: task_title
        expr: TASK_TITLE
        data_type: VARCHAR
      - name: is_billable
        expr: IS_BILLABLE
        data_type: BOOLEAN
      - name: is_approved
        expr: IS_APPROVED
        data_type: BOOLEAN
    time_dimensions:
      - name: entry_date
        expr: DATE_PERFORMED
        data_type: DATE
        synonyms: [work_date]
    facts:
      - name: hours
        expr: TIME_IN_HOURS
        data_type: NUMBER
        synonyms: [time_hours]
      - name: revenue
        expr: REVENUE_DOLLARS
        data_type: NUMBER
      - name: cost
        expr: COST_DOLLARS
        data_type: NUMBER
      - name: rate
        expr: RATE_DOLLARS
        data_type: NUMBER
    metrics:
      - name: total_hours
        expr: SUM(TIME_IN_HOURS)
      - name: billable_hours
        expr: SUM(CASE WHEN IS_BILLABLE THEN TIME_IN_HOURS ELSE 0 END)
      - name: utilization_pct
        expr: ROUND(100.0 * SUM(CASE WHEN IS_BILLABLE THEN TIME_IN_HOURS ELSE 0 END) / NULLIF(SUM(TIME_IN_HOURS), 0), 2)
      - name: total_time_revenue
        expr: SUM(REVENUE_DOLLARS)
      - name: total_time_cost
        expr: SUM(COST_DOLLARS)
      - name: entry_count
        expr: COUNT(*)
    filters:
      - name: billable_only
        expr: "IS_BILLABLE = TRUE"

  - name: project_status
    description: Project status and health fact table
    base_table:
      database: ANALYTICS
      schema: MART
      table: FACT_PROJECT_STATUS
    dimensions:
      - name: workspace_id
        expr: WORKSPACE_ID
        data_type: NUMBER
        unique: true
        synonyms: [project_id]
      - name: project_name
        expr: PROJECT_NAME
        data_type: VARCHAR
      - name: project_status
        expr: PROJECT_STATUS
        data_type: VARCHAR
        synonyms: [status]
      - name: customer_name
        expr: CUSTOMER_NAME
        data_type: VARCHAR
        synonyms: [account_name]
      - name: health_status
        expr: HEALTH_STATUS
        data_type: VARCHAR
        synonyms: [risk_status]
      - name: is_overdue
        expr: IS_OVERDUE
        data_type: BOOLEAN
    time_dimensions:
      - name: start_date
        expr: START_DATE
        data_type: DATE
      - name: due_date
        expr: DUE_DATE
        data_type: DATE
    facts:
      - name: percentage_complete
        expr: PERCENTAGE_COMPLETE
        data_type: NUMBER
      - name: budget_dollars
        expr: BUDGET_DOLLARS
        data_type: NUMBER
        synonyms: [budget]
      - name: budget_used
        expr: BUDGET_USED_DOLLARS
        data_type: NUMBER
      - name: budget_remaining
        expr: BUDGET_REMAINING_DOLLARS
        data_type: NUMBER
      - name: budget_consumed_pct
        expr: BUDGET_CONSUMED_PCT
        data_type: NUMBER
      - name: burn_rate_index
        expr: BURN_RATE_INDEX
        data_type: NUMBER
      - name: total_hours
        expr: TOTAL_HOURS
        data_type: NUMBER
      - name: billable_hours
        expr: BILLABLE_HOURS
        data_type: NUMBER
      - name: total_revenue
        expr: TOTAL_BILLABLE_REVENUE
        data_type: NUMBER
      - name: total_cost
        expr: TOTAL_COST
        data_type: NUMBER
      - name: deal_amount
        expr: DEAL_AMOUNT
        data_type: NUMBER
    metrics:
      - name: status_project_count
        expr: COUNT(*)
      - name: status_total_budget
        expr: SUM(BUDGET_DOLLARS)
      - name: avg_completion
        expr: AVG(PERCENTAGE_COMPLETE)
      - name: overdue_count
        expr: SUM(CASE WHEN IS_OVERDUE THEN 1 ELSE 0 END)
      - name: at_risk_count
        expr: SUM(CASE WHEN HEALTH_STATUS LIKE 'At Risk%' THEN 1 ELSE 0 END)
    filters:
      - name: at_risk_projects
        expr: "HEALTH_STATUS LIKE 'At Risk%'"
      - name: active_projects
        expr: "PROJECT_STATUS = 'active'"

  - name: deal_to_delivery
    description: Cross-system fact linking Salesforce deals to Mavenlink projects
    base_table:
      database: ANALYTICS
      schema: MART
      table: FACT_DEAL_TO_DELIVERY
    dimensions:
      - name: opportunity_id
        expr: OPPORTUNITY_ID
        data_type: VARCHAR
        unique: true
      - name: workspace_id
        expr: WORKSPACE_ID
        data_type: NUMBER
        synonyms: [project_id]
      - name: account_id
        expr: ACCOUNT_ID
        data_type: VARCHAR
      - name: account_name
        expr: ACCOUNT_NAME
        data_type: VARCHAR
        synonyms: [customer_name]
      - name: industry
        expr: INDUSTRY
        data_type: VARCHAR
      - name: deal_name
        expr: DEAL_NAME
        data_type: VARCHAR
      - name: deal_stage
        expr: DEAL_STAGE
        data_type: VARCHAR
      - name: deal_is_won
        expr: DEAL_IS_WON
        data_type: BOOLEAN
      - name: sales_rep_name
        expr: SALES_REP_NAME
        data_type: VARCHAR
        synonyms: [rep_name, rep]
      - name: project_name
        expr: PROJECT_NAME
        data_type: VARCHAR
      - name: project_status
        expr: PROJECT_STATUS
        data_type: VARCHAR
    time_dimensions:
      - name: deal_close_date
        expr: DEAL_CLOSE_DATE
        data_type: DATE
      - name: project_start_date
        expr: PROJECT_START_DATE
        data_type: DATE
      - name: project_due_date
        expr: PROJECT_DUE_DATE
        data_type: DATE
    facts:
      - name: deal_amount
        expr: DEAL_AMOUNT
        data_type: NUMBER
        synonyms: [revenue, deal_value]
      - name: project_budget
        expr: PROJECT_BUDGET
        data_type: NUMBER
      - name: project_spend
        expr: PROJECT_SPEND
        data_type: NUMBER
      - name: project_revenue
        expr: PROJECT_REVENUE
        data_type: NUMBER
      - name: project_cost
        expr: PROJECT_COST
        data_type: NUMBER
      - name: project_total_hours
        expr: PROJECT_TOTAL_HOURS
        data_type: NUMBER
      - name: project_billable_hours
        expr: PROJECT_BILLABLE_HOURS
        data_type: NUMBER
      - name: margin_pct
        expr: MARGIN_PCT
        data_type: NUMBER
        synonyms: [profitability, margin]
      - name: days_deal_to_kickoff
        expr: DAYS_DEAL_TO_KICKOFF
        data_type: NUMBER
    metrics:
      - name: total_deal_value
        expr: SUM(DEAL_AMOUNT)
      - name: total_project_revenue
        expr: SUM(PROJECT_REVENUE)
      - name: total_project_cost
        expr: SUM(PROJECT_COST)
      - name: total_margin
        expr: SUM(PROJECT_REVENUE) - SUM(PROJECT_COST)
      - name: avg_margin_pct
        expr: AVG(MARGIN_PCT)
      - name: linked_deal_count
        expr: COUNT(*)
      - name: avg_days_to_kickoff
        expr: AVG(DAYS_DEAL_TO_KICKOFF)
    filters:
      - name: won_deals
        expr: "DEAL_IS_WON = TRUE"

relationships:
  - name: opportunity_to_customer
    left_table: opportunities
    right_table: customers
    relationship_columns:
      - left_column: ACCOUNT_ID
        right_column: ACCOUNT_ID

  - name: line_item_to_opportunity
    left_table: line_items
    right_table: opportunities
    relationship_columns:
      - left_column: OPPORTUNITY_ID
        right_column: OPPORTUNITY_ID

  - name: line_item_to_product
    left_table: line_items
    right_table: products
    relationship_columns:
      - left_column: PRODUCT_ID
        right_column: PRODUCT_ID

  - name: time_entry_to_project
    left_table: time_entries
    right_table: projects
    relationship_columns:
      - left_column: WORKSPACE_ID
        right_column: WORKSPACE_ID

  - name: project_status_to_project
    left_table: project_status
    right_table: projects
    relationship_columns:
      - left_column: WORKSPACE_ID
        right_column: WORKSPACE_ID

  - name: deal_to_delivery_to_customer
    left_table: deal_to_delivery
    right_table: customers
    relationship_columns:
      - left_column: ACCOUNT_ID
        right_column: ACCOUNT_ID

  - name: project_to_customer
    left_table: projects
    right_table: customers
    relationship_columns:
      - left_column: LINKED_ACCOUNT_ID
        right_column: ACCOUNT_ID

  - name: opportunity_to_rep
    left_table: opportunities
    right_table: users
    relationship_columns:
      - left_column: OWNER_ID
        right_column: SF_USER_ID

  - name: time_entry_to_user
    left_table: time_entries
    right_table: users
    relationship_columns:
      - left_column: USER_ID
        right_column: ML_USER_ID

verified_queries:
  - name: top_customers_by_revenue
    question: Who are our top 10 customers by revenue?
    use_as_onboarding_question: true
    sql: |
      SELECT ACCOUNT_NAME as customer, TOTAL_WON_REVENUE as revenue
      FROM ANALYTICS.MART.DIM_CUSTOMER
      WHERE TOTAL_WON_REVENUE > 0
      ORDER BY TOTAL_WON_REVENUE DESC LIMIT 10

  - name: pipeline_by_stage
    question: What is our pipeline by sales stage?
    use_as_onboarding_question: true
    sql: |
      SELECT STAGE_NAME as stage, COUNT(*) as deals, SUM(AMOUNT) as value
      FROM ANALYTICS.MART.FACT_OPPORTUNITY
      WHERE IS_CLOSED = FALSE
      GROUP BY STAGE_NAME ORDER BY value DESC

  - name: consultant_utilization
    question: What is the utilization rate of our consultants?
    use_as_onboarding_question: true
    sql: |
      SELECT FULL_NAME as consultant, ROUND(ML_UTILIZATION_PCT, 1) as utilization_pct, 
             ROUND(ML_BILLABLE_HOURS, 1) as billable_hours, ROUND(ML_TOTAL_HOURS, 1) as total_hours
      FROM ANALYTICS.MART.DIM_USER
      WHERE ML_TOTAL_HOURS > 0
      ORDER BY ML_UTILIZATION_PCT DESC

  - name: at_risk_projects
    question: Which projects are at risk?
    use_as_onboarding_question: true
    sql: |
      SELECT PROJECT_NAME, HEALTH_STATUS, CUSTOMER_NAME, 
             ROUND(BUDGET_CONSUMED_PCT, 1) as budget_pct, PERCENTAGE_COMPLETE as completion_pct
      FROM ANALYTICS.MART.FACT_PROJECT_STATUS
      WHERE HEALTH_STATUS LIKE 'At Risk%'
      ORDER BY BURN_RATE_INDEX DESC

  - name: profitability_by_customer
    question: Which customers have the best project margins?
    use_as_onboarding_question: true
    sql: |
      SELECT ACCOUNT_NAME as customer, COUNT(*) as projects, 
             ROUND(AVG(MARGIN_PCT), 2) as avg_margin_pct,
             SUM(PROJECT_REVENUE) - SUM(PROJECT_COST) as total_margin
      FROM ANALYTICS.MART.FACT_DEAL_TO_DELIVERY
      WHERE DEAL_IS_WON = TRUE
      GROUP BY ACCOUNT_NAME ORDER BY avg_margin_pct DESC

  - name: revenue_by_product_family
    question: What products generate the most revenue?
    sql: |
      SELECT PRODUCT_FAMILY as family, COUNT(*) as line_items, SUM(TOTAL_PRICE) as revenue
      FROM ANALYTICS.MART.FACT_OPPORTUNITY_LINE_ITEM
      WHERE OPPORTUNITY_IS_WON = TRUE
      GROUP BY PRODUCT_FAMILY ORDER BY revenue DESC

  - name: win_rate_by_rep
    question: What is the win rate by sales rep?
    sql: |
      SELECT OWNER_NAME as rep,
             COUNT(*) as total_deals,
             SUM(CASE WHEN IS_WON THEN 1 ELSE 0 END) as won,
             ROUND(100.0 * SUM(CASE WHEN IS_WON THEN 1 ELSE 0 END) / COUNT(*), 1) as win_rate_pct
      FROM ANALYTICS.MART.FACT_OPPORTUNITY
      WHERE IS_CLOSED = TRUE AND OWNER_NAME IS NOT NULL
      GROUP BY OWNER_NAME ORDER BY win_rate_pct DESC

  - name: overdue_projects
    question: Which projects are overdue?
    sql: |
      SELECT PROJECT_NAME, CUSTOMER_NAME, DUE_DATE, PERCENTAGE_COMPLETE
      FROM ANALYTICS.MART.FACT_PROJECT_STATUS
      WHERE IS_OVERDUE = TRUE AND PROJECT_STATUS = 'active'
      ORDER BY DUE_DATE
$$);

-- ============================================================================
-- AGENT 2: HYBRID APPROACH (Sales + Delivery views)
-- ============================================================================

CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML('ANALYTICS.MART', $$
name: SALES_ANALYTICS
description: Sales-focused semantic view for Salesforce CRM data analysis

tables:
  - name: customers
    base_table:
      database: ANALYTICS
      schema: MART
      table: DIM_CUSTOMER
    dimensions:
      - name: account_id
        expr: ACCOUNT_ID
        data_type: VARCHAR
        unique: true
      - name: customer_name
        expr: ACCOUNT_NAME
        data_type: VARCHAR
        synonyms: [account_name, company_name]
      - name: industry
        expr: INDUSTRY
        data_type: VARCHAR
    facts:
      - name: total_won_revenue
        expr: TOTAL_WON_REVENUE
        data_type: NUMBER
      - name: open_opportunities
        expr: OPEN_OPPORTUNITIES
        data_type: NUMBER
    metrics:
      - name: customer_revenue
        expr: SUM(TOTAL_WON_REVENUE)

  - name: sales_reps
    base_table:
      database: ANALYTICS
      schema: MART
      table: DIM_USER
    dimensions:
      - name: user_key
        expr: USER_KEY
        data_type: VARCHAR
        unique: true
      - name: sf_user_id
        expr: SF_USER_ID
        data_type: VARCHAR
      - name: rep_name
        expr: FULL_NAME
        data_type: VARCHAR
        synonyms: [sales_rep, owner_name]
      - name: title
        expr: TITLE
        data_type: VARCHAR
      - name: department
        expr: DEPARTMENT
        data_type: VARCHAR

  - name: products
    base_table:
      database: ANALYTICS
      schema: MART
      table: DIM_PRODUCT
    dimensions:
      - name: product_id
        expr: PRODUCT_ID
        data_type: VARCHAR
        unique: true
      - name: product_name
        expr: PRODUCT_NAME
        data_type: VARCHAR
      - name: product_family
        expr: PRODUCT_FAMILY
        data_type: VARCHAR
        synonyms: [category]
      - name: is_active
        expr: IS_ACTIVE
        data_type: BOOLEAN

  - name: opportunities
    base_table:
      database: ANALYTICS
      schema: MART
      table: FACT_OPPORTUNITY
    dimensions:
      - name: opportunity_id
        expr: OPPORTUNITY_ID
        data_type: VARCHAR
        unique: true
      - name: account_id
        expr: ACCOUNT_ID
        data_type: VARCHAR
      - name: owner_id
        expr: OWNER_ID
        data_type: VARCHAR
      - name: opportunity_name
        expr: OPPORTUNITY_NAME
        data_type: VARCHAR
        synonyms: [deal_name]
      - name: account_name
        expr: ACCOUNT_NAME
        data_type: VARCHAR
      - name: stage_name
        expr: STAGE_NAME
        data_type: VARCHAR
        synonyms: [stage]
      - name: owner_name
        expr: OWNER_NAME
        data_type: VARCHAR
        synonyms: [sales_rep, rep]
      - name: is_won
        expr: IS_WON
        data_type: BOOLEAN
      - name: is_closed
        expr: IS_CLOSED
        data_type: BOOLEAN
      - name: opportunity_type
        expr: OPPORTUNITY_TYPE
        data_type: VARCHAR
      - name: forecast_category
        expr: FORECAST_CATEGORY
        data_type: VARCHAR
    time_dimensions:
      - name: close_date
        expr: CLOSE_DATE
        data_type: DATE
    facts:
      - name: amount
        expr: AMOUNT
        data_type: NUMBER
        synonyms: [deal_value, revenue]
      - name: probability
        expr: PROBABILITY
        data_type: NUMBER
      - name: days_to_close
        expr: DAYS_TO_CLOSE
        data_type: NUMBER
    metrics:
      - name: total_pipeline
        expr: SUM(AMOUNT)
      - name: total_won
        expr: SUM(CASE WHEN IS_WON THEN AMOUNT ELSE 0 END)
      - name: opportunity_count
        expr: COUNT(*)
      - name: win_rate
        expr: ROUND(100.0 * SUM(CASE WHEN IS_WON THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN IS_CLOSED THEN 1 ELSE 0 END), 0), 2)
      - name: avg_deal_size
        expr: AVG(AMOUNT)
    filters:
      - name: open_pipeline
        expr: "IS_CLOSED = FALSE"
      - name: won_deals
        expr: "IS_WON = TRUE"

  - name: line_items
    base_table:
      database: ANALYTICS
      schema: MART
      table: FACT_OPPORTUNITY_LINE_ITEM
    dimensions:
      - name: line_item_id
        expr: OPPORTUNITY_LINE_ITEM_ID
        data_type: VARCHAR
        unique: true
      - name: opportunity_id
        expr: OPPORTUNITY_ID
        data_type: VARCHAR
      - name: product_id
        expr: PRODUCT_ID
        data_type: VARCHAR
      - name: product_name
        expr: PRODUCT_NAME
        data_type: VARCHAR
      - name: product_family
        expr: PRODUCT_FAMILY
        data_type: VARCHAR
      - name: opportunity_is_won
        expr: OPPORTUNITY_IS_WON
        data_type: BOOLEAN
    time_dimensions:
      - name: close_date
        expr: OPPORTUNITY_CLOSE_DATE
        data_type: DATE
    facts:
      - name: quantity
        expr: QUANTITY
        data_type: NUMBER
      - name: total_price
        expr: TOTAL_PRICE
        data_type: NUMBER
        synonyms: [revenue]
      - name: discount_percent
        expr: DISCOUNT_PERCENT
        data_type: NUMBER
    metrics:
      - name: total_line_revenue
        expr: SUM(TOTAL_PRICE)
      - name: line_item_count
        expr: COUNT(*)

relationships:
  - name: opportunity_to_customer
    left_table: opportunities
    right_table: customers
    relationship_columns:
      - left_column: ACCOUNT_ID
        right_column: ACCOUNT_ID
  - name: line_item_to_opportunity
    left_table: line_items
    right_table: opportunities
    relationship_columns:
      - left_column: OPPORTUNITY_ID
        right_column: OPPORTUNITY_ID
  - name: line_item_to_product
    left_table: line_items
    right_table: products
    relationship_columns:
      - left_column: PRODUCT_ID
        right_column: PRODUCT_ID

verified_queries:
  - name: pipeline_by_stage
    question: What is our pipeline by stage?
    use_as_onboarding_question: true
    sql: |
      SELECT STAGE_NAME as stage, COUNT(*) as deals, SUM(AMOUNT) as value
      FROM ANALYTICS.MART.FACT_OPPORTUNITY
      WHERE IS_CLOSED = FALSE
      GROUP BY STAGE_NAME ORDER BY value DESC
  - name: top_customers
    question: Who are our top customers by revenue?
    use_as_onboarding_question: true
    sql: |
      SELECT ACCOUNT_NAME as customer, TOTAL_WON_REVENUE as revenue
      FROM ANALYTICS.MART.DIM_CUSTOMER
      WHERE TOTAL_WON_REVENUE > 0
      ORDER BY TOTAL_WON_REVENUE DESC LIMIT 10
  - name: rep_performance
    question: How are sales reps performing?
    use_as_onboarding_question: true
    sql: |
      SELECT OWNER_NAME as rep, COUNT(*) as total_deals,
             SUM(CASE WHEN IS_WON THEN 1 ELSE 0 END) as won,
             SUM(CASE WHEN IS_WON THEN AMOUNT ELSE 0 END) as revenue
      FROM ANALYTICS.MART.FACT_OPPORTUNITY
      WHERE OWNER_NAME IS NOT NULL AND IS_CLOSED = TRUE
      GROUP BY OWNER_NAME ORDER BY revenue DESC
  - name: revenue_by_product
    question: What products generate the most revenue?
    use_as_onboarding_question: true
    sql: |
      SELECT PRODUCT_FAMILY as family, SUM(TOTAL_PRICE) as revenue
      FROM ANALYTICS.MART.FACT_OPPORTUNITY_LINE_ITEM
      WHERE OPPORTUNITY_IS_WON = TRUE
      GROUP BY PRODUCT_FAMILY ORDER BY revenue DESC
$$);

CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML('ANALYTICS.MART', $$
name: DELIVERY_ANALYTICS
description: Delivery-focused semantic view for Mavenlink project and resource analysis

tables:
  - name: consultants
    base_table:
      database: ANALYTICS
      schema: MART
      table: DIM_USER
    dimensions:
      - name: user_key
        expr: USER_KEY
        data_type: VARCHAR
        unique: true
      - name: ml_user_id
        expr: ML_USER_ID
        data_type: NUMBER
        unique: true
      - name: consultant_name
        expr: FULL_NAME
        data_type: VARCHAR
        synonyms: [user_name, resource_name]
      - name: title
        expr: TITLE
        data_type: VARCHAR
      - name: department
        expr: DEPARTMENT
        data_type: VARCHAR
    facts:
      - name: total_hours
        expr: ML_TOTAL_HOURS
        data_type: NUMBER
      - name: billable_hours
        expr: ML_BILLABLE_HOURS
        data_type: NUMBER
      - name: utilization_pct
        expr: ML_UTILIZATION_PCT
        data_type: NUMBER
        synonyms: [utilization]
      - name: active_projects
        expr: ML_ACTIVE_PROJECTS
        data_type: NUMBER
    metrics:
      - name: avg_utilization
        expr: AVG(ML_UTILIZATION_PCT)
      - name: consultant_count
        expr: COUNT(DISTINCT ML_USER_ID)

  - name: projects
    base_table:
      database: ANALYTICS
      schema: MART
      table: DIM_PROJECT
    dimensions:
      - name: workspace_id
        expr: WORKSPACE_ID
        data_type: NUMBER
        unique: true
        synonyms: [project_id]
      - name: project_name
        expr: PROJECT_NAME
        data_type: VARCHAR
      - name: project_status
        expr: PROJECT_STATUS
        data_type: VARCHAR
        synonyms: [status]
      - name: is_overdue
        expr: IS_OVERDUE
        data_type: BOOLEAN
    time_dimensions:
      - name: start_date
        expr: START_DATE
        data_type: DATE
      - name: due_date
        expr: DUE_DATE
        data_type: DATE
    facts:
      - name: budget_dollars
        expr: BUDGET_DOLLARS
        data_type: NUMBER
      - name: budget_used
        expr: BUDGET_USED_DOLLARS
        data_type: NUMBER
      - name: total_hours
        expr: TOTAL_HOURS
        data_type: NUMBER
      - name: percentage_complete
        expr: PERCENTAGE_COMPLETE
        data_type: NUMBER
    metrics:
      - name: project_count
        expr: COUNT(*)
      - name: total_budget
        expr: SUM(BUDGET_DOLLARS)

  - name: time_entries
    base_table:
      database: ANALYTICS
      schema: MART
      table: FACT_TIME_ENTRY
    dimensions:
      - name: time_entry_id
        expr: TIME_ENTRY_ID
        data_type: NUMBER
        unique: true
      - name: workspace_id
        expr: WORKSPACE_ID
        data_type: NUMBER
        synonyms: [project_id]
      - name: user_id
        expr: USER_ID
        data_type: NUMBER
      - name: user_name
        expr: USER_NAME
        data_type: VARCHAR
        synonyms: [consultant_name]
      - name: project_title
        expr: PROJECT_TITLE
        data_type: VARCHAR
      - name: is_billable
        expr: IS_BILLABLE
        data_type: BOOLEAN
      - name: is_approved
        expr: IS_APPROVED
        data_type: BOOLEAN
    time_dimensions:
      - name: entry_date
        expr: DATE_PERFORMED
        data_type: DATE
    facts:
      - name: hours
        expr: TIME_IN_HOURS
        data_type: NUMBER
      - name: revenue
        expr: REVENUE_DOLLARS
        data_type: NUMBER
      - name: cost
        expr: COST_DOLLARS
        data_type: NUMBER
    metrics:
      - name: total_hours
        expr: SUM(TIME_IN_HOURS)
      - name: billable_hours
        expr: SUM(CASE WHEN IS_BILLABLE THEN TIME_IN_HOURS ELSE 0 END)
      - name: utilization_pct
        expr: ROUND(100.0 * SUM(CASE WHEN IS_BILLABLE THEN TIME_IN_HOURS ELSE 0 END) / NULLIF(SUM(TIME_IN_HOURS), 0), 2)
      - name: total_revenue
        expr: SUM(REVENUE_DOLLARS)
      - name: total_cost
        expr: SUM(COST_DOLLARS)

  - name: project_status
    base_table:
      database: ANALYTICS
      schema: MART
      table: FACT_PROJECT_STATUS
    dimensions:
      - name: workspace_id
        expr: WORKSPACE_ID
        data_type: NUMBER
        unique: true
      - name: project_name
        expr: PROJECT_NAME
        data_type: VARCHAR
      - name: health_status
        expr: HEALTH_STATUS
        data_type: VARCHAR
        synonyms: [status, risk_status]
      - name: customer_name
        expr: CUSTOMER_NAME
        data_type: VARCHAR
      - name: is_overdue
        expr: IS_OVERDUE
        data_type: BOOLEAN
      - name: project_status
        expr: PROJECT_STATUS
        data_type: VARCHAR
    facts:
      - name: percentage_complete
        expr: PERCENTAGE_COMPLETE
        data_type: NUMBER
      - name: budget_consumed_pct
        expr: BUDGET_CONSUMED_PCT
        data_type: NUMBER
      - name: burn_rate_index
        expr: BURN_RATE_INDEX
        data_type: NUMBER
    metrics:
      - name: project_count
        expr: COUNT(*)
      - name: at_risk_count
        expr: SUM(CASE WHEN HEALTH_STATUS LIKE 'At Risk%' THEN 1 ELSE 0 END)
    filters:
      - name: at_risk
        expr: "HEALTH_STATUS LIKE 'At Risk%'"
      - name: active
        expr: "PROJECT_STATUS = 'active'"

  - name: deal_to_delivery
    base_table:
      database: ANALYTICS
      schema: MART
      table: FACT_DEAL_TO_DELIVERY
    dimensions:
      - name: opportunity_id
        expr: OPPORTUNITY_ID
        data_type: VARCHAR
        unique: true
      - name: workspace_id
        expr: WORKSPACE_ID
        data_type: NUMBER
      - name: account_name
        expr: ACCOUNT_NAME
        data_type: VARCHAR
        synonyms: [customer_name]
      - name: sales_rep_name
        expr: SALES_REP_NAME
        data_type: VARCHAR
        synonyms: [rep]
      - name: project_name
        expr: PROJECT_NAME
        data_type: VARCHAR
      - name: industry
        expr: INDUSTRY
        data_type: VARCHAR
      - name: deal_is_won
        expr: DEAL_IS_WON
        data_type: BOOLEAN
    facts:
      - name: deal_amount
        expr: DEAL_AMOUNT
        data_type: NUMBER
      - name: project_revenue
        expr: PROJECT_REVENUE
        data_type: NUMBER
      - name: project_cost
        expr: PROJECT_COST
        data_type: NUMBER
      - name: margin_pct
        expr: MARGIN_PCT
        data_type: NUMBER
        synonyms: [profitability]
    metrics:
      - name: total_margin
        expr: SUM(PROJECT_REVENUE) - SUM(PROJECT_COST)
      - name: avg_margin_pct
        expr: AVG(MARGIN_PCT)
      - name: deal_count
        expr: COUNT(*)
    filters:
      - name: won_deals
        expr: "DEAL_IS_WON = TRUE"

relationships:
  - name: time_to_project
    left_table: time_entries
    right_table: projects
    relationship_columns:
      - left_column: WORKSPACE_ID
        right_column: WORKSPACE_ID
  - name: project_status_to_project
    left_table: project_status
    right_table: projects
    relationship_columns:
      - left_column: WORKSPACE_ID
        right_column: WORKSPACE_ID

  - name: time_entry_to_consultant
    left_table: time_entries
    right_table: consultants
    relationship_columns:
      - left_column: USER_ID
        right_column: ML_USER_ID

verified_queries:
  - name: consultant_utilization
    question: What is the utilization rate of our consultants?
    use_as_onboarding_question: true
    sql: |
      SELECT FULL_NAME as consultant, ROUND(ML_UTILIZATION_PCT, 1) as utilization_pct,
             ROUND(ML_BILLABLE_HOURS, 1) as billable_hours
      FROM ANALYTICS.MART.DIM_USER
      WHERE ML_TOTAL_HOURS > 0
      ORDER BY ML_UTILIZATION_PCT DESC
  - name: at_risk_projects
    question: Which projects are at risk?
    use_as_onboarding_question: true
    sql: |
      SELECT PROJECT_NAME, HEALTH_STATUS, CUSTOMER_NAME, ROUND(BUDGET_CONSUMED_PCT, 1) as budget_pct
      FROM ANALYTICS.MART.FACT_PROJECT_STATUS
      WHERE HEALTH_STATUS LIKE 'At Risk%'
      ORDER BY BURN_RATE_INDEX DESC
  - name: profitability_by_customer
    question: Which customers have the best margins?
    use_as_onboarding_question: true
    sql: |
      SELECT ACCOUNT_NAME as customer, COUNT(*) as projects, ROUND(AVG(MARGIN_PCT), 2) as margin
      FROM ANALYTICS.MART.FACT_DEAL_TO_DELIVERY
      WHERE DEAL_IS_WON = TRUE
      GROUP BY ACCOUNT_NAME ORDER BY margin DESC
  - name: hours_this_month
    question: How many hours were logged this month?
    use_as_onboarding_question: true
    sql: |
      SELECT ROUND(SUM(TIME_IN_HOURS), 1) as total_hours,
             ROUND(SUM(CASE WHEN IS_BILLABLE THEN TIME_IN_HOURS ELSE 0 END), 1) as billable_hours
      FROM ANALYTICS.MART.FACT_TIME_ENTRY
      WHERE DATE_PERFORMED >= DATE_TRUNC('MONTH', CURRENT_DATE())
$$);

-- ============================================================================
-- AGENT 3: PER-FACT APPROACH (5 granular views)
-- ============================================================================

CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML('ANALYTICS.MART', $$
name: OPPORTUNITIES_VIEW
description: Per-fact semantic view for Sales Opportunities

tables:
  - name: opportunities
    base_table:
      database: ANALYTICS
      schema: MART
      table: FACT_OPPORTUNITY
    dimensions:
      - name: opportunity_id
        expr: OPPORTUNITY_ID
        data_type: VARCHAR
        unique: true
      - name: account_id
        expr: ACCOUNT_ID
        data_type: VARCHAR
      - name: opportunity_name
        expr: OPPORTUNITY_NAME
        data_type: VARCHAR
        synonyms: [deal_name]
      - name: account_name
        expr: ACCOUNT_NAME
        data_type: VARCHAR
        synonyms: [customer_name]
      - name: stage_name
        expr: STAGE_NAME
        data_type: VARCHAR
        synonyms: [stage]
      - name: owner_name
        expr: OWNER_NAME
        data_type: VARCHAR
        synonyms: [sales_rep, rep]
      - name: is_won
        expr: IS_WON
        data_type: BOOLEAN
      - name: is_closed
        expr: IS_CLOSED
        data_type: BOOLEAN
      - name: opportunity_type
        expr: OPPORTUNITY_TYPE
        data_type: VARCHAR
      - name: forecast_category
        expr: FORECAST_CATEGORY
        data_type: VARCHAR
      - name: has_linked_project
        expr: HAS_LINKED_PROJECT
        data_type: BOOLEAN
    time_dimensions:
      - name: close_date
        expr: CLOSE_DATE
        data_type: DATE
    facts:
      - name: amount
        expr: AMOUNT
        data_type: NUMBER
        synonyms: [deal_value, revenue]
      - name: probability
        expr: PROBABILITY
        data_type: NUMBER
      - name: days_to_close
        expr: DAYS_TO_CLOSE
        data_type: NUMBER
    metrics:
      - name: total_pipeline
        expr: SUM(AMOUNT)
      - name: total_won
        expr: SUM(CASE WHEN IS_WON THEN AMOUNT ELSE 0 END)
      - name: opportunity_count
        expr: COUNT(*)
      - name: win_rate
        expr: ROUND(100.0 * SUM(CASE WHEN IS_WON THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN IS_CLOSED THEN 1 ELSE 0 END), 0), 2)
      - name: avg_deal_size
        expr: AVG(AMOUNT)
    filters:
      - name: open
        expr: "IS_CLOSED = FALSE"
      - name: won
        expr: "IS_WON = TRUE"

verified_queries:
  - name: pipeline_by_stage
    question: Pipeline by stage?
    use_as_onboarding_question: true
    sql: |
      SELECT STAGE_NAME as stage, COUNT(*) as deals, SUM(AMOUNT) as value
      FROM ANALYTICS.MART.FACT_OPPORTUNITY
      WHERE IS_CLOSED = FALSE
      GROUP BY STAGE_NAME ORDER BY value DESC
  - name: top_deals
    question: What are the largest deals?
    sql: |
      SELECT OPPORTUNITY_NAME as deal, ACCOUNT_NAME as customer, AMOUNT, STAGE_NAME as stage
      FROM ANALYTICS.MART.FACT_OPPORTUNITY
      WHERE IS_CLOSED = FALSE
      ORDER BY AMOUNT DESC LIMIT 10
$$);

CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML('ANALYTICS.MART', $$
name: LINE_ITEMS_VIEW
description: Per-fact semantic view for Opportunity Line Items

tables:
  - name: line_items
    base_table:
      database: ANALYTICS
      schema: MART
      table: FACT_OPPORTUNITY_LINE_ITEM
    dimensions:
      - name: line_item_id
        expr: OPPORTUNITY_LINE_ITEM_ID
        data_type: VARCHAR
        unique: true
      - name: opportunity_id
        expr: OPPORTUNITY_ID
        data_type: VARCHAR
      - name: product_id
        expr: PRODUCT_ID
        data_type: VARCHAR
      - name: product_name
        expr: PRODUCT_NAME
        data_type: VARCHAR
      - name: product_family
        expr: PRODUCT_FAMILY
        data_type: VARCHAR
        synonyms: [category]
      - name: opportunity_is_won
        expr: OPPORTUNITY_IS_WON
        data_type: BOOLEAN
    time_dimensions:
      - name: close_date
        expr: OPPORTUNITY_CLOSE_DATE
        data_type: DATE
    facts:
      - name: total_price
        expr: TOTAL_PRICE
        data_type: NUMBER
        synonyms: [revenue]
      - name: quantity
        expr: QUANTITY
        data_type: NUMBER
      - name: discount_percent
        expr: DISCOUNT_PERCENT
        data_type: NUMBER
    metrics:
      - name: total_revenue
        expr: SUM(TOTAL_PRICE)
      - name: total_quantity
        expr: SUM(QUANTITY)
      - name: avg_discount
        expr: AVG(DISCOUNT_PERCENT)
      - name: line_item_count
        expr: COUNT(*)
    filters:
      - name: won
        expr: "OPPORTUNITY_IS_WON = TRUE"

verified_queries:
  - name: revenue_by_family
    question: Revenue by product family?
    use_as_onboarding_question: true
    sql: |
      SELECT PRODUCT_FAMILY as family, SUM(TOTAL_PRICE) as revenue, COUNT(*) as items
      FROM ANALYTICS.MART.FACT_OPPORTUNITY_LINE_ITEM
      WHERE OPPORTUNITY_IS_WON = TRUE
      GROUP BY PRODUCT_FAMILY ORDER BY revenue DESC
  - name: top_products
    question: Top products by revenue?
    sql: |
      SELECT PRODUCT_NAME, SUM(TOTAL_PRICE) as revenue
      FROM ANALYTICS.MART.FACT_OPPORTUNITY_LINE_ITEM
      WHERE OPPORTUNITY_IS_WON = TRUE
      GROUP BY PRODUCT_NAME ORDER BY revenue DESC LIMIT 10
$$);

CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML('ANALYTICS.MART', $$
name: TIME_ENTRIES_VIEW
description: Per-fact semantic view for Time Entries

tables:
  - name: time_entries
    base_table:
      database: ANALYTICS
      schema: MART
      table: FACT_TIME_ENTRY
    dimensions:
      - name: time_entry_id
        expr: TIME_ENTRY_ID
        data_type: NUMBER
        unique: true
      - name: workspace_id
        expr: WORKSPACE_ID
        data_type: NUMBER
        synonyms: [project_id]
      - name: user_id
        expr: USER_ID
        data_type: NUMBER
      - name: user_name
        expr: USER_NAME
        data_type: VARCHAR
        synonyms: [consultant_name, consultant]
      - name: project_title
        expr: PROJECT_TITLE
        data_type: VARCHAR
        synonyms: [project_name, project]
      - name: project_status
        expr: PROJECT_STATUS
        data_type: VARCHAR
      - name: task_title
        expr: TASK_TITLE
        data_type: VARCHAR
      - name: is_billable
        expr: IS_BILLABLE
        data_type: BOOLEAN
      - name: is_approved
        expr: IS_APPROVED
        data_type: BOOLEAN
    time_dimensions:
      - name: entry_date
        expr: DATE_PERFORMED
        data_type: DATE
        synonyms: [work_date]
    facts:
      - name: hours
        expr: TIME_IN_HOURS
        data_type: NUMBER
      - name: revenue
        expr: REVENUE_DOLLARS
        data_type: NUMBER
      - name: cost
        expr: COST_DOLLARS
        data_type: NUMBER
      - name: rate
        expr: RATE_DOLLARS
        data_type: NUMBER
    metrics:
      - name: total_hours
        expr: SUM(TIME_IN_HOURS)
      - name: billable_hours
        expr: SUM(CASE WHEN IS_BILLABLE THEN TIME_IN_HOURS ELSE 0 END)
      - name: utilization_pct
        expr: ROUND(100.0 * SUM(CASE WHEN IS_BILLABLE THEN TIME_IN_HOURS ELSE 0 END) / NULLIF(SUM(TIME_IN_HOURS), 0), 2)
      - name: total_revenue
        expr: SUM(REVENUE_DOLLARS)
      - name: total_cost
        expr: SUM(COST_DOLLARS)
      - name: entry_count
        expr: COUNT(*)
    filters:
      - name: billable
        expr: "IS_BILLABLE = TRUE"

verified_queries:
  - name: hours_by_consultant
    question: Hours by consultant?
    use_as_onboarding_question: true
    sql: |
      SELECT USER_NAME as consultant, ROUND(SUM(TIME_IN_HOURS), 1) as hours
      FROM ANALYTICS.MART.FACT_TIME_ENTRY
      GROUP BY USER_NAME ORDER BY hours DESC
  - name: hours_by_project
    question: Hours by project?
    sql: |
      SELECT PROJECT_TITLE as project, ROUND(SUM(TIME_IN_HOURS), 1) as hours
      FROM ANALYTICS.MART.FACT_TIME_ENTRY
      GROUP BY PROJECT_TITLE ORDER BY hours DESC
$$);

CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML('ANALYTICS.MART', $$
name: PROJECT_STATUS_VIEW
description: Per-fact semantic view for Project Status and Health

tables:
  - name: project_status
    base_table:
      database: ANALYTICS
      schema: MART
      table: FACT_PROJECT_STATUS
    dimensions:
      - name: workspace_id
        expr: WORKSPACE_ID
        data_type: NUMBER
        unique: true
        synonyms: [project_id]
      - name: project_name
        expr: PROJECT_NAME
        data_type: VARCHAR
      - name: project_status
        expr: PROJECT_STATUS
        data_type: VARCHAR
        synonyms: [status]
      - name: health_status
        expr: HEALTH_STATUS
        data_type: VARCHAR
        synonyms: [risk_status, health]
      - name: customer_name
        expr: CUSTOMER_NAME
        data_type: VARCHAR
      - name: is_overdue
        expr: IS_OVERDUE
        data_type: BOOLEAN
    time_dimensions:
      - name: start_date
        expr: START_DATE
        data_type: DATE
      - name: due_date
        expr: DUE_DATE
        data_type: DATE
    facts:
      - name: percentage_complete
        expr: PERCENTAGE_COMPLETE
        data_type: NUMBER
      - name: budget_dollars
        expr: BUDGET_DOLLARS
        data_type: NUMBER
        synonyms: [budget]
      - name: budget_consumed_pct
        expr: BUDGET_CONSUMED_PCT
        data_type: NUMBER
      - name: burn_rate_index
        expr: BURN_RATE_INDEX
        data_type: NUMBER
      - name: total_hours
        expr: TOTAL_HOURS
        data_type: NUMBER
      - name: total_revenue
        expr: TOTAL_BILLABLE_REVENUE
        data_type: NUMBER
      - name: total_cost
        expr: TOTAL_COST
        data_type: NUMBER
    metrics:
      - name: project_count
        expr: COUNT(*)
      - name: total_budget
        expr: SUM(BUDGET_DOLLARS)
      - name: avg_completion
        expr: AVG(PERCENTAGE_COMPLETE)
      - name: overdue_count
        expr: SUM(CASE WHEN IS_OVERDUE THEN 1 ELSE 0 END)
      - name: at_risk_count
        expr: SUM(CASE WHEN HEALTH_STATUS LIKE 'At Risk%' THEN 1 ELSE 0 END)
    filters:
      - name: at_risk
        expr: "HEALTH_STATUS LIKE 'At Risk%'"
      - name: active
        expr: "PROJECT_STATUS = 'active'"

verified_queries:
  - name: at_risk_projects
    question: Which projects are at risk?
    use_as_onboarding_question: true
    sql: |
      SELECT PROJECT_NAME, HEALTH_STATUS, CUSTOMER_NAME, ROUND(BUDGET_CONSUMED_PCT, 1) as budget_pct
      FROM ANALYTICS.MART.FACT_PROJECT_STATUS
      WHERE HEALTH_STATUS LIKE 'At Risk%'
      ORDER BY BURN_RATE_INDEX DESC
  - name: project_health_summary
    question: Project health summary?
    sql: |
      SELECT HEALTH_STATUS, COUNT(*) as count
      FROM ANALYTICS.MART.FACT_PROJECT_STATUS
      GROUP BY HEALTH_STATUS ORDER BY count DESC
$$);

CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML('ANALYTICS.MART', $$
name: DEAL_TO_DELIVERY_VIEW
description: Per-fact semantic view for cross-system deal profitability

tables:
  - name: deal_to_delivery
    base_table:
      database: ANALYTICS
      schema: MART
      table: FACT_DEAL_TO_DELIVERY
    dimensions:
      - name: opportunity_id
        expr: OPPORTUNITY_ID
        data_type: VARCHAR
        unique: true
      - name: workspace_id
        expr: WORKSPACE_ID
        data_type: NUMBER
        synonyms: [project_id]
      - name: account_id
        expr: ACCOUNT_ID
        data_type: VARCHAR
      - name: account_name
        expr: ACCOUNT_NAME
        data_type: VARCHAR
        synonyms: [customer_name, customer]
      - name: industry
        expr: INDUSTRY
        data_type: VARCHAR
      - name: deal_name
        expr: DEAL_NAME
        data_type: VARCHAR
      - name: deal_stage
        expr: DEAL_STAGE
        data_type: VARCHAR
      - name: deal_is_won
        expr: DEAL_IS_WON
        data_type: BOOLEAN
      - name: sales_rep_name
        expr: SALES_REP_NAME
        data_type: VARCHAR
        synonyms: [rep, sales_rep]
      - name: project_name
        expr: PROJECT_NAME
        data_type: VARCHAR
      - name: project_status
        expr: PROJECT_STATUS
        data_type: VARCHAR
    time_dimensions:
      - name: deal_close_date
        expr: DEAL_CLOSE_DATE
        data_type: DATE
      - name: project_start_date
        expr: PROJECT_START_DATE
        data_type: DATE
    facts:
      - name: deal_amount
        expr: DEAL_AMOUNT
        data_type: NUMBER
        synonyms: [revenue, deal_value]
      - name: project_revenue
        expr: PROJECT_REVENUE
        data_type: NUMBER
      - name: project_cost
        expr: PROJECT_COST
        data_type: NUMBER
      - name: margin_pct
        expr: MARGIN_PCT
        data_type: NUMBER
        synonyms: [profitability, margin]
      - name: project_total_hours
        expr: PROJECT_TOTAL_HOURS
        data_type: NUMBER
      - name: days_deal_to_kickoff
        expr: DAYS_DEAL_TO_KICKOFF
        data_type: NUMBER
    metrics:
      - name: total_deal_value
        expr: SUM(DEAL_AMOUNT)
      - name: total_project_revenue
        expr: SUM(PROJECT_REVENUE)
      - name: total_project_cost
        expr: SUM(PROJECT_COST)
      - name: total_margin
        expr: SUM(PROJECT_REVENUE) - SUM(PROJECT_COST)
      - name: avg_margin_pct
        expr: AVG(MARGIN_PCT)
      - name: deal_count
        expr: COUNT(*)
    filters:
      - name: won
        expr: "DEAL_IS_WON = TRUE"

verified_queries:
  - name: profitability_by_customer
    question: Customer profitability?
    use_as_onboarding_question: true
    sql: |
      SELECT ACCOUNT_NAME as customer, COUNT(*) as projects, ROUND(AVG(MARGIN_PCT), 2) as avg_margin
      FROM ANALYTICS.MART.FACT_DEAL_TO_DELIVERY
      WHERE DEAL_IS_WON = TRUE
      GROUP BY ACCOUNT_NAME ORDER BY avg_margin DESC
  - name: rep_performance
    question: Rep delivery performance?
    use_as_onboarding_question: true
    sql: |
      SELECT SALES_REP_NAME as rep, COUNT(*) as deals, ROUND(AVG(MARGIN_PCT), 2) as avg_margin
      FROM ANALYTICS.MART.FACT_DEAL_TO_DELIVERY
      WHERE DEAL_IS_WON = TRUE AND SALES_REP_NAME IS NOT NULL
      GROUP BY SALES_REP_NAME ORDER BY avg_margin DESC
  - name: margin_by_industry
    question: Margin by industry?
    sql: |
      SELECT COALESCE(INDUSTRY, 'Unknown') as industry, ROUND(AVG(MARGIN_PCT), 2) as avg_margin
      FROM ANALYTICS.MART.FACT_DEAL_TO_DELIVERY
      WHERE DEAL_IS_WON = TRUE
      GROUP BY INDUSTRY ORDER BY avg_margin DESC
$$);

-- ============================================================================
-- VERIFICATION
-- ============================================================================

SHOW SEMANTIC VIEWS IN SCHEMA ANALYTICS.MART;

SELECT 'SUCCESS: All 8 semantic views deployed' AS status;
