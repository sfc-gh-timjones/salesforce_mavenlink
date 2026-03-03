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

### Step 2: Import dbt Project

1. Navigate to **Data » Workspaces**
2. Click **Create Workspace** → **From Git Repository**
3. Enter repository URL: `https://github.com/sfc-gh-timjones/salesforce_mavenlink.git`
4. Set path to: `Clean Star Schema/clean_star`
5. Click **Create**

### Step 3: Run dbt Project

1. Open the workspace created in Step 2
2. Click **Run** to execute all models
3. Verify all 17 models complete successfully

### Step 4: Deploy Semantic View

Run in a SQL worksheet (copy the entire block):

```sql
CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(
  'ANALYTICS2.MART',
  $$
<PASTE CONTENTS OF semantic_views/opportunity_delivery_analytics.yaml HERE>
  $$
);
```

Verify deployment:
```sql
SHOW SEMANTIC VIEWS IN ANALYTICS2.MART;
DESC SEMANTIC VIEW ANALYTICS2.MART.OPPORTUNITY_DELIVERY_ANALYTICS;
```

### Step 5: Create Cortex Agent

Run in a SQL worksheet:

```sql
CREATE OR REPLACE AGENT ANALYTICS2.AGENTS.OPPORTUNITY_DELIVERY_AGENT
  COMMENT = 'Agent for opportunity delivery analytics - analyzes deal profitability, project delivery, and cross-system metrics'
  FROM SPECIFICATION $$
  {
    "models": {"orchestration": "auto"},
    "instructions": {
      "orchestration": "You analyze opportunity delivery data spanning Salesforce CRM and Mavenlink project management. Use the semantic view to answer questions about sales pipeline, deal profitability, project delivery, and cross-system analytics.",
      "response": "You are a Revenue Operations analyst specializing in deal-to-delivery analytics. Lead with bullet points summarizing key insights. Use bold text for important metrics. Show data tables after insights."
    },
    "tools": [{
      "tool_spec": {
        "type": "cortex_analyst_text_to_sql",
        "name": "opportunity_delivery",
        "description": "Query opportunity and delivery data - sales pipeline, customers, products, projects, and profitability metrics."
      }
    }],
    "tool_resources": {
      "opportunity_delivery": {
        "semantic_view": "ANALYTICS2.MART.OPPORTUNITY_DELIVERY_ANALYTICS",
        "execution_environment": {"type": "warehouse", "warehouse": "WH_XS"}
      }
    }
  }
  $$;
```

Verify:
```sql
DESCRIBE AGENT ANALYTICS2.AGENTS.OPPORTUNITY_DELIVERY_AGENT;
```

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
SELECT * FROM ANALYTICS2.MART.OPPORTUNITY_DELIVERY_ANALYTICS LIMIT 10;
```

## Sample Questions for Agent

Once deployed, ask the agent:

- "What is our total pipeline value by stage?"
- "Who are the top 5 sales reps by closed-won revenue?"
- "Which projects are at risk and over budget?"
- "What is our average margin by product family?"
- "Show me deals closed this quarter with their delivery status"

## Files Reference

| File | Purpose |
|------|---------|
| `clean_star/` | dbt project with all models |
| `semantic_views/opportunity_delivery_analytics.yaml` | Semantic view definition |
| `create_agent.sql` | Agent creation script |
| `deploy_semantic_view.py` | Python deployment script (CLI alternative) |
| `erd.html` | Visual ERD diagram |
| `reference_guide.md` | Detailed technical reference |

## Troubleshooting

**dbt models fail**: Check that source tables exist in FIVETRAN_DATABASE

**Semantic view error**: Ensure all mart tables exist before deploying

**Agent not responding**: Verify semantic view is deployed and warehouse is running
