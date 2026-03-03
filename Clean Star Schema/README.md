# Clean Star Schema - Deployment Guide

Deploy a star schema analytics model with Cortex Agent for opportunity delivery analytics.

## What's Included

- **dbt Project**: 17 models (staging views, intermediate aggregations, dimension tables, fact table)
- **Semantic View**: OPPORTUNITY_DELIVERY_ANALYTICS with verified queries
- **Cortex Agent**: OPPORTUNITY_DELIVERY_AGENT for natural language analytics

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     ANALYTICS2.MART                             │
│  ┌─────────────┐     ┌─────────────────────────────┐           │
│  │DIM_CUSTOMER │────▶│                             │           │
│  └─────────────┘     │                             │           │
│  ┌─────────────┐     │  FACT_OPPORTUNITY_DELIVERY  │           │
│  │DIM_USER     │────▶│       (125K+ rows)          │           │
│  └─────────────┘     │                             │           │
│  ┌─────────────┐     │                             │           │
│  │DIM_PRODUCT  │────▶│                             │           │
│  └─────────────┘     │                             │           │
│  ┌─────────────┐     │                             │           │
│  │DIM_PROJECT  │────▶│                             │           │
│  └─────────────┘     └─────────────────────────────┘           │
└─────────────────────────────────────────────────────────────────┘
```

## Prerequisites

Source data must exist in:
- `FIVETRAN_DATABASE.SALESFORCE` - Opportunity, Account, User, Product_2, Opportunity_Line_Item
- `FIVETRAN_DATABASE.MAVENLINK` - Workspace, Time_Entry, Story, User

## Deployment Steps (Snowsight)

### Step 1: Create Warehouse, Database & Schemas

Run in a SQL worksheet:

```sql
-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS WH_XS 
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

-- Create database and schemas
CREATE DATABASE IF NOT EXISTS ANALYTICS2;
CREATE SCHEMA IF NOT EXISTS ANALYTICS2.MART;
CREATE SCHEMA IF NOT EXISTS ANALYTICS2.STG;
CREATE SCHEMA IF NOT EXISTS ANALYTICS2.INT;
CREATE SCHEMA IF NOT EXISTS ANALYTICS2.AGENTS;
```

### Step 2: Set Up Git Integration

Create the API integration for GitHub (one-time setup, requires ACCOUNTADMIN):

```sql
CREATE OR REPLACE API INTEGRATION GIT_INTEGRATION
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/')
  ENABLED = TRUE;
```

### Step 3: Import dbt Project from Git

1. Navigate to **Projects » Workspaces**
2. Click on the workspace name dropdown at the top, then select **Create from Git Repository**
3. Enter repository URL: `https://github.com/sfc-gh-timjones/salesforce_mavenlink`
4. Select `GIT_INTEGRATION` as the API integration
5. Click **Create**

### Step 4: Deploy and Run dbt Project

1. The workspace will open automatically after creation
2. Click **Compile** (top of workspace) to compile the project
3. Click the dropdown next to Compile, select **Run**, then click **Run** to execute all models
4. Verify all 17 models complete successfully

### Step 5: Deploy Semantic View

Open `deploy_semantic_view.sql` in a Snowsight worksheet and run it (select all, then run).

Verify deployment:
```sql
SHOW SEMANTIC VIEWS IN ANALYTICS2.MART;
DESC SEMANTIC VIEW ANALYTICS2.MART.OPPORTUNITY_DELIVERY_ANALYTICS;
```

### Step 6: Create Cortex Agent

Open `create_agent.sql` in a Snowsight worksheet and run it.

Verify:
```sql
DESCRIBE AGENT ANALYTICS2.AGENTS.OPPORTUNITY_DELIVERY_AGENT;
```

To use the agent in Snowflake Intelligence:
1. Navigate to **AI & ML » Agents**
2. Click the **Snowflake Intelligence** tab at the top
3. Click **Add existing agent** (top right)
4. Select `OPPORTUNITY_DELIVERY_AGENT` and add it
5. Start asking questions!

## Verification

After deployment, test with these queries:

```sql
-- Check fact table
SELECT COUNT(*) FROM ANALYTICS2.MART.FACT_OPPORTUNITY_DELIVERY;

-- Check dimensions
SELECT COUNT(*) FROM ANALYTICS2.MART.DIM_CUSTOMER;
SELECT COUNT(*) FROM ANALYTICS2.MART.DIM_USER;
SELECT COUNT(*) FROM ANALYTICS2.MART.DIM_PRODUCT;
SELECT COUNT(*) FROM ANALYTICS2.MART.DIM_PROJECT;

-- Test semantic view
SELECT * FROM SEMANTIC_VIEW(
  ANALYTICS2.MART.OPPORTUNITY_DELIVERY_ANALYTICS
  DIMENSIONS FACT_OPPORTUNITY_DELIVERY.OPPORTUNITY_NAME, 
             FACT_OPPORTUNITY_DELIVERY.ACCOUNT_NAME,
             FACT_OPPORTUNITY_DELIVERY.STAGE_NAME
  FACTS FACT_OPPORTUNITY_DELIVERY.DEAL_AMOUNT
)
LIMIT 10;
```

## Sample Questions

> **Note**: This demo uses synthetic data. Logic and null rates may differ from real production data.

Go to **AI & ML » Snowflake Intelligence**, make sure `OPPORTUNITY_DELIVERY_AGENT` is selected, then try these questions:

- Show me sales pipeline performance by rep including win rates, deal velocity, and pipeline health metrics.
- Which products drive the most revenue and what are the typical deal structures, discounts, and line item patterns?
- Show project delivery performance and resource utilization metrics broken down by project status.
- Who are the top project leads by revenue and what is their track record on budget and delivery?
- Which deals show significant revenue leakage between what was sold and what was actually delivered?
- How does the speed of handoff from sales close to project kickoff impact delivery success and margin?
- Which sales reps have the best win rates AND delivery outcomes - who are the full-cycle performers?
- Which customers have the highest lifetime value when you combine their sales bookings with delivery profitability?

## Files Reference

| File | Purpose |
|------|---------|
| `clean_star/` | dbt project with all models |
| `create_agent.sql` | Agent creation script |
| `deploy_semantic_view.sql` | Semantic view deployment script |
| `erd.html` | Visual ERD diagram (download to local machine to view) |

## Troubleshooting

**dbt models fail**: Check that source tables exist in FIVETRAN_DATABASE

**Semantic view error**: Ensure all mart tables exist before deploying

**Agent not responding**: Verify semantic view is deployed
