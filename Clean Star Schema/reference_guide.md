# Clean Star Schema - Reference Guide

## Overview
This guide provides step-by-step instructions for deploying and running the Clean Star Schema dbt project using Snowflake CLI, along with semantic view deployment guidance.

## Prerequisites

### 1. Connection Configuration
The project uses the `coco` connection for PAT-based authentication. Ensure your `connections.toml` file is properly configured.

## Database Setup

### 1. Create Required Databases and Schemas
```sql
-- Create the analytics database for dbt models
CREATE DATABASE IF NOT EXISTS ANALYTICS2;
CREATE SCHEMA IF NOT EXISTS ANALYTICS2.MART;
CREATE SCHEMA IF NOT EXISTS ANALYTICS2.STG;
CREATE SCHEMA IF NOT EXISTS ANALYTICS2.INT;

-- Grant permissions to your role
GRANT ALL ON DATABASE ANALYTICS2 TO ROLE <YOUR_ROLE>;
GRANT ALL ON ALL SCHEMAS IN DATABASE ANALYTICS2 TO ROLE <YOUR_ROLE>;
```

### 2. External Access Integration (Required for dbt Packages)
Before deploying the dbt project, you must create an external access integration to allow dbt to download packages from the internet:

```sql
-- Create network rule for dbt package repositories
CREATE OR REPLACE NETWORK RULE dbt_external_access_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('hub.getdbt.com', 'codeload.github.com', 'github.com', 'api.github.com');

-- Create external access integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION DBT_EXTERNAL_ACCESS
  ALLOWED_NETWORK_RULES = (dbt_external_access_rule)
  ENABLED = true;

-- Grant usage to your role
GRANT USAGE ON INTEGRATION DBT_EXTERNAL_ACCESS TO ROLE <YOUR_ROLE>;
```

**Note**: All four domains (`hub.getdbt.com`, `codeload.github.com`, `github.com`, `api.github.com`) are required for dbt to successfully download packages like `dbt_utils`.

## dbt Project Deployment

### 1. Deploy the dbt Project
```bash
# Navigate to the project directory
cd "/path/to/Clean Star Schema"

# Deploy the dbt project to Snowflake with external access integration
snow dbt deploy clean_star \
  --source "/path/to/Clean Star Schema/clean_star" \
  --connection coco \
  --database ANALYTICS2 \
  --schema PUBLIC \
  --external-access-integration DBT_EXTERNAL_ACCESS \
  --force
```

This command will:
- Upload the dbt project to Snowflake
- Install required packages (like `dbt_utils`) using the external access integration
- Create the dbt project object in `ANALYTICS2.PUBLIC`
- Prepare the project for execution

### 2. Execute the dbt Project
```bash
# Run all dbt models
snow dbt execute --connection coco --database ANALYTICS2 --schema PUBLIC clean_star run

# Alternative: Run specific commands
snow dbt execute --connection coco --database ANALYTICS2 --schema PUBLIC clean_star "dbt run --models staging"
snow dbt execute --connection coco --database ANALYTICS2 --schema PUBLIC clean_star "dbt run --models marts"
snow dbt execute --connection coco --database ANALYTICS2 --schema PUBLIC clean_star test
```

### 3. Verify Deployment
Check that all models have been created successfully:
```sql
-- Check staging models
SELECT * FROM ANALYTICS2.STG.STG_SALESFORCE__OPPORTUNITY LIMIT 5;
SELECT * FROM ANALYTICS2.STG.STG_MAVENLINK__WORKSPACE LIMIT 5;

-- Check dimension models
SELECT * FROM ANALYTICS2.MART.DIM_CUSTOMER LIMIT 5;
SELECT * FROM ANALYTICS2.MART.DIM_USER LIMIT 5;

-- Check fact models
SELECT * FROM ANALYTICS2.MART.FACT_OPPORTUNITY_DELIVERY LIMIT 5;
```


## Semantic View Deployment

### Current Status: ✅ Fully Working

The semantic view is fully deployed with all features including relationships and verified queries.

- **Semantic View**: `ANALYTICS2.MART.OPPORTUNITY_DELIVERY_ANALYTICS`
- **Tables**: 5 (1 fact + 4 dimensions)
- **Relationships**: 4 (fact to each dimension)
- **Verified Queries**: 10

### YAML Format Requirements (CRITICAL)

When creating semantic view YAML files, use the correct syntax. The `SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML` procedure has specific format requirements:

#### ✅ Correct Relationship Format
```yaml
relationships:
  - name: FACT_TO_CUSTOMER
    left_table: FACT_OPPORTUNITY_DELIVERY
    right_table: DIM_CUSTOMER
    relationship_columns:
      - left_column: ACCOUNT_ID
        right_column: ACCOUNT_ID
    relationship_type: many_to_one
    join_type: left_outer  # Always specify join_type
```

#### ❌ Incorrect Format (Will Cause "Invalid identifier ''" Error)
```yaml
relationships:
  - name: fact_to_customer
    from:
      table: fact_opportunity_delivery
      column: ACCOUNT_ID
    to:
      table: dim_customer
      column: ACCOUNT_ID
    type: many_to_one
```

### Best Practices Checklist

When creating semantic views, follow these best practices from the Cortex Analyst optimization skills:

#### Primary Keys & Relationships
- ✅ All tables must have `primary_key` defined
- ✅ Add `unique: true` to primary key dimension columns
- ✅ Relationships use `left_table/right_table` with `relationship_columns`
- ✅ Always specify `join_type` (`left_outer` or `inner`)
- ✅ Use `many_to_one` or `one_to_one` (not deprecated types)

#### Column Metadata
- ✅ All columns must have `description`
- ✅ All columns must have `data_type`
- ✅ Add `synonyms` for business terminology variants
- ✅ Add `sample_values` for categorical/enum columns
- ✅ Mark unique columns with `unique: true`

#### Filters
- ✅ Add named `filters` for common WHERE conditions
- ✅ Filter expressions must be quoted strings

#### Example with Best Practices
```yaml
dimensions:
  - name: OPPORTUNITY_ID
    description: Unique Salesforce opportunity identifier. Primary key for the fact table.
    expr: OPPORTUNITY_ID
    data_type: VARCHAR
    unique: true  # Mark PKs as unique
    synonyms:
      - deal_id
      - opp_id

  - name: STAGE_NAME
    description: Current sales stage in the pipeline
    expr: STAGE_NAME
    data_type: VARCHAR
    sample_values:  # Add sample values for enums
      - Closed Won
      - Closed Lost
      - Discovery
      - Negotiation

filters:  # Add named filters for common patterns
  - name: won_deals
    description: Closed-won opportunities only
    expr: "IS_WON = TRUE"
  - name: at_risk_projects
    description: Projects with at-risk health status
    expr: "PROJECT_HEALTH_STATUS LIKE 'At Risk%'"
```

#### Verified Queries ARE Supported
Verified queries work in semantic views via YAML. Include them at the root level:
```yaml
verified_queries:
  - name: my_query
    question: What is the total revenue by customer?
    verified_by: System
    verified_at: 1740700800
    use_as_onboarding_question: true
    sql: |
      SELECT customer, SUM(revenue) as total_revenue
      FROM my_table
      GROUP BY customer
```

### Deployment Commands

```bash
# Deploy semantic view
cd "/path/to/Clean Star Schema"
SNOWFLAKE_CONNECTION_NAME=coco python deploy_semantic_view.py
```

### Deployment Script (`deploy_semantic_view.py`)
```python
import os
import snowflake.connector

yaml_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    'semantic_views',
    'opportunity_delivery_analytics.yaml',
)

with open(yaml_path, 'r') as f:
    yaml_content = f.read()

yaml_escaped = yaml_content.replace("'", "''")

conn = snowflake.connector.connect(
    connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "coco"
)

try:
    cursor = conn.cursor()
    cursor.execute("USE ROLE ACCOUNTADMIN")
    cursor.execute("USE DATABASE ANALYTICS2")
    cursor.execute("USE SCHEMA MART")

    sql = f"CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML('ANALYTICS2.MART', '{yaml_escaped}')"
    cursor.execute(sql)
    result = cursor.fetchone()
    print(f"Result: {result[0]}")
except Exception as e:
    print(f"Error: {e}")
finally:
    cursor.close()
    conn.close()
```

### Verify Deployment
```sql
-- Check semantic view exists
SHOW SEMANTIC VIEWS IN ANALYTICS2.MART;

-- Describe semantic view (see all dimensions, facts, metrics, verified queries)
DESC SEMANTIC VIEW ANALYTICS2.MART.OPPORTUNITY_DELIVERY_ANALYTICS;
```

## Troubleshooting

### dbt Deployment Issues
1. **External Access Integration Error**: Ensure the external access integration is created and granted to your role
2. **Permission Errors**: Verify your role has appropriate database and schema permissions
3. **Connection Issues**: Check your `connections.toml` configuration

### Semantic View Issues
1. **"Invalid identifier ''" Error**: Check relationship format - must use `left_table`/`right_table` with `relationship_columns`, NOT `from`/`to` syntax
2. **Column Not Found**: Verify all column names exist in the underlying tables
3. **YAML Syntax**: Ensure proper YAML formatting and escaping of special characters
4. **Table Names**: Use UPPERCASE for table names in relationships to match Snowflake identifiers

### Common Commands for Debugging
```bash
# Check dbt project status
snow dbt list-projects --connection coco

# Verify connection
snow sql -c coco -q "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE()"

# Check deployed models
snow sql -c coco -q "SHOW TABLES IN SCHEMA ANALYTICS2.MART"

# Check dbt project details
snow sql -c coco -q "SHOW DBT PROJECTS IN SCHEMA ANALYTICS2.PUBLIC"

# View dbt project execution history
snow sql -c coco -q "SELECT * FROM INFORMATION_SCHEMA.DBT_PROJECT_EXECUTION_HISTORY WHERE PROJECT_NAME = 'CLEAN_STAR'"
```

## Next Steps / TODO

### Medium Priority
1. **Add More Verified Queries**: Expand the set of verified queries for better onboarding
2. **Optimize Performance**: Review and optimize model performance
3. **Add Data Quality Tests**: Implement dbt tests for data validation

### Documentation
1. **Add Query Examples**: Provide more examples of complex cross-system queries
2. **Create Troubleshooting Guide**: Expand troubleshooting section based on common issues

## Support
For issues or questions:
1. Check the troubleshooting section above
2. Verify all prerequisites are met
3. Review Snowflake CLI and dbt documentation
4. Check semantic view syntax against Snowflake documentation

---
*Last updated: March 2026*
*Status: dbt project fully working, semantic view fully working with relationships and verified queries*