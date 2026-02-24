# Revenue Operations - Semantic Views for Snowflake Intelligence

This folder contains semantic view configurations for A/B testing different agent architectures with Snowflake Intelligence. Three configurations are provided to compare how different semantic model structures perform for natural language queries.

## Prerequisites

Before deploying these semantic views, ensure:
1. The dbt project in `revenue_operations/` has been run successfully
2. The ANALYTICS database exists with STG, INT, and MART schemas
3. The following mart tables exist:
   - `ANALYTICS.MART.DIM_CUSTOMER`
   - `ANALYTICS.MART.DIM_USER`
   - `ANALYTICS.MART.DIM_PRODUCT`
   - `ANALYTICS.MART.DIM_PROJECT`
   - `ANALYTICS.MART.DIM_DATE`
   - `ANALYTICS.MART.FACT_OPPORTUNITY`
   - `ANALYTICS.MART.FACT_OPPORTUNITY_LINE_ITEM`
   - `ANALYTICS.MART.FACT_TIME_ENTRY`
   - `ANALYTICS.MART.FACT_PROJECT_STATUS`
   - `ANALYTICS.MART.FACT_DEAL_TO_DELIVERY`

## Folder Structure

```
semantic_views/
├── README.md                      # This file
├── deploy.sql                     # SQL script to deploy semantic views
├── create_agents.sql              # SQL script to create Cortex Agents
├── setup.sql                      # (deprecated - use deploy.sql)
│
├── agent1_unified/                # Configuration 1: Single unified view
│   └── revenue_operations_unified.yaml
│
├── agent2_hybrid/                 # Configuration 2: Two domain-specific views
│   ├── sales_analytics.yaml
│   └── delivery_analytics.yaml
│
└── agent3_per_fact/              # Configuration 3: Five fact-specific views
    ├── opportunities_view.yaml
    ├── line_items_view.yaml
    ├── time_entries_view.yaml
    ├── project_status_view.yaml
    └── deal_to_delivery_view.yaml
```

## Agent Configurations

### Agent 1: Unified Approach
**File:** `agent1_unified/revenue_operations_unified.yaml`

A single comprehensive semantic view containing all 10 tables (5 dimensions + 5 facts) with full relationships, metrics, and verified queries.

**Pros:**
- Single point of context for the LLM
- All cross-system queries naturally supported
- Simplest agent configuration

**Cons:**
- Larger context window consumption
- May be slower for simple single-table queries
- More complex relationship graph

**Best for:** Questions that span multiple domains or require cross-system analysis

---

### Agent 2: Hybrid Approach
**Files:** `agent2_hybrid/sales_analytics.yaml` and `agent2_hybrid/delivery_analytics.yaml`

Two domain-specific semantic views:
1. **Sales Analytics** - Salesforce-focused: Customers, Opportunities, Products, Line Items
2. **Delivery Analytics** - Mavenlink-focused: Projects, Time Entries, Consultants, plus cross-system Deal-to-Delivery

**Pros:**
- Smaller context per query
- Domain separation may improve accuracy for domain-specific questions
- Delivery view includes the flagship `FACT_DEAL_TO_DELIVERY` for cross-system queries

**Cons:**
- Two views to manage
- Agent needs routing logic (can use multiple semantic views in one agent)

**Best for:** Organizations with distinct Sales and Delivery teams who ask domain-specific questions

---

### Agent 3: Per-Fact Approach
**Files:** `agent3_per_fact/*.yaml` (5 files)

Five granular semantic views, one per fact table:
1. `opportunities_view.yaml` - Sales pipeline and deals
2. `line_items_view.yaml` - Products sold on opportunities
3. `time_entries_view.yaml` - Consultant time tracking
4. `project_status_view.yaml` - Project health and status
5. `deal_to_delivery_view.yaml` - Cross-system profitability

**Pros:**
- Smallest context per query
- Highly focused verified queries
- May perform best for targeted single-table questions

**Cons:**
- Most views to manage
- Cross-system queries require the right view to be selected
- More agent configuration complexity

**Best for:** Testing whether granular views improve accuracy for specific question types

---

## Deployment Instructions

### Option 1: Using the Deployment Scripts (Recommended)

**Step 1: Deploy Semantic Views**
```bash
snowsql -c <connection_name> -f deploy.sql
```

**Step 2: Create Cortex Agents**
```bash
snowsql -c <connection_name> -f create_agents.sql
```

Or run both in Snowsight worksheets.

### Option 2: Manual Deployment

For each YAML file, use the Snowflake stored procedure:

```sql
-- First parameter is DATABASE.SCHEMA, view name comes from YAML 'name' field
CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(
    'ANALYTICS.MART',
    ${YAML content here}$
);
```

### Option 3: Using Snowsight UI

1. Navigate to Data > Semantic Views in Snowsight
2. Click "Create Semantic View"
3. Upload the YAML file or paste its contents
4. Review and create

---

## Creating Agents in Snowflake Intelligence

After deploying the semantic views, create agents:

### Agent 1: Unified
```sql
-- Create an agent using the unified semantic view
-- In Snowsight: Intelligence > Agents > Create Agent
-- Select semantic view: ANALYTICS.MART.REVENUE_OPERATIONS_UNIFIED
```

### Agent 2: Hybrid
```sql
-- Create an agent with both sales and delivery semantic views
-- Select semantic views:
--   ANALYTICS.MART.SALES_ANALYTICS
--   ANALYTICS.MART.DELIVERY_ANALYTICS
```

### Agent 3: Per-Fact
```sql
-- Create an agent with all 5 fact-based semantic views
-- Select semantic views:
--   ANALYTICS.MART.OPPORTUNITIES_VIEW
--   ANALYTICS.MART.LINE_ITEMS_VIEW
--   ANALYTICS.MART.TIME_ENTRIES_VIEW
--   ANALYTICS.MART.PROJECT_STATUS_VIEW
--   ANALYTICS.MART.DEAL_TO_DELIVERY_VIEW
```

---

## Testing Questions

Use these questions to test each agent configuration:

### Sales Questions (All agents should answer)
1. "What is our pipeline by sales stage?"
2. "Who are our top 10 customers by revenue?"
3. "How are sales reps performing this year?"
4. "What products generate the most revenue?"

### Delivery Questions (All agents should answer)
1. "What is the utilization rate of our consultants?"
2. "Which projects are at risk?"
3. "How many hours were logged this month?"
4. "What is our average project margin?"

### Cross-System Questions (Tests cross-system capability)
1. "Which customers have the best project margins?"
2. "Which sales reps sell deals that deliver profitably?"
3. "What top customers have no recent project activity?"
4. "What is the margin by customer industry?"

### Complex Questions (Stress test)
1. "Show me customers from our top 20 by revenue who haven't had project activity in 6 months"
2. "Compare deal value to actual project revenue by sales rep"
3. "Which active projects are over budget but under 50% complete?"

---

## Verified Queries (VQRs)

Each semantic view includes verified queries to improve accuracy:

| Agent | Total VQRs | Onboarding Questions |
|-------|------------|---------------------|
| Unified | 12 | 5 |
| Hybrid (Sales) | 8 | 3 |
| Hybrid (Delivery) | 10 | 4 |
| Per-Fact (Total) | 11 | 8 |

Verified queries serve as "golden examples" that help Cortex Analyst generate accurate SQL for similar questions.

---

## Customization for Your Environment

If deploying in a different environment:

1. **Update database/schema references** in all YAML files:
   - Find: `ANALYTICS.MART`
   - Replace with: `YOUR_DATABASE.YOUR_SCHEMA`

2. **Update verified query SQL** to match your schema names

3. **Add sample_values** for enum dimensions if your data has different values

4. **Add custom filters** based on your business requirements

---

## Troubleshooting

### Semantic View Creation Fails
- Verify all base tables exist and are accessible
- Check column names match exactly (case-sensitive)
- Ensure relationships reference valid columns

### Agent Returns Incorrect SQL
- Review verified queries for similar questions
- Add more synonyms to dimensions and metrics
- Check filter expressions are valid SQL

### Cross-System Queries Don't Work
- Ensure relationships are properly defined
- For Per-Fact approach, use the `deal_to_delivery_view` for cross-system questions

---

## Support

For questions about:
- **dbt models**: See `revenue_operations/README.md`
- **Data sources**: See `planning.md` in the project root
- **Snowflake Intelligence**: https://docs.snowflake.com/en/user-guide/snowflake-cortex

---

*Generated: February 2026*
