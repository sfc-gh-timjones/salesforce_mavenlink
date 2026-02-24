/*
 * Cortex Agents - Creation Script
 * 
 * Creates 3 Cortex Agents for A/B testing different semantic view strategies.
 * Run AFTER deploy.sql has created the semantic views.
 *
 * Prerequisites:
 *   - 8 semantic views created in ANALYTICS.MART
 *   - CREATE AGENT privilege
 */

USE ROLE ACCOUNTADMIN;
USE DATABASE ANALYTICS;
USE SCHEMA MART;
USE WAREHOUSE WH_XS;

-- ============================================================================
-- AGENT 1: UNIFIED (Single semantic view with all tables)
-- ============================================================================

CREATE OR REPLACE AGENT ANALYTICS.MART.REVOPS_AGENT_UNIFIED
  COMMENT = 'Agent 1: Unified approach - single semantic view with all tables'
  FROM SPECIFICATION $$
  {
    "models": {
      "orchestration": "claude-3-5-sonnet"
    },
    "instructions": {
      "orchestration": "Use the unified semantic view for all revenue operations questions.",
      "response": "You are a Revenue Operations analyst. Provide clear, actionable insights with supporting data. Key capabilities: Sales pipeline analysis, customer revenue, deal-to-delivery profitability, cross-system metrics."
    },
    "tools": [
      {
        "tool_spec": {
          "type": "cortex_analyst_text_to_sql",
          "name": "revops_unified",
          "description": "Query all revenue operations data - sales pipeline, customers, and project profitability"
        }
      }
    ],
    "tool_resources": {
      "revops_unified": {
        "semantic_view": "ANALYTICS.MART.REVENUE_OPERATIONS_UNIFIED",
        "execution_environment": {
          "type": "warehouse",
          "warehouse": "WH_XS"
        }
      }
    }
  }
  $$;

-- ============================================================================
-- AGENT 2: HYBRID (Sales + Delivery semantic views)
-- ============================================================================

CREATE OR REPLACE AGENT ANALYTICS.MART.REVOPS_AGENT_HYBRID
  COMMENT = 'Agent 2: Hybrid approach - separate Sales and Delivery views'
  FROM SPECIFICATION $$
  {
    "models": {
      "orchestration": "claude-3-5-sonnet"
    },
    "instructions": {
      "orchestration": "Route questions appropriately: Use sales_analytics for CRM/pipeline questions. Use delivery_analytics for project/time/profitability questions. For cross-functional questions, query both and synthesize.",
      "response": "You are a Revenue Operations analyst with expertise in both sales and delivery. Be concise and data-driven."
    },
    "tools": [
      {
        "tool_spec": {
          "type": "cortex_analyst_text_to_sql",
          "name": "sales_analytics",
          "description": "Query sales data - opportunities, customers, pipeline, and reps"
        }
      },
      {
        "tool_spec": {
          "type": "cortex_analyst_text_to_sql",
          "name": "delivery_analytics",
          "description": "Query delivery data - projects, time entries, utilization, and deal profitability"
        }
      }
    ],
    "tool_resources": {
      "sales_analytics": {
        "semantic_view": "ANALYTICS.MART.SALES_ANALYTICS",
        "execution_environment": {
          "type": "warehouse",
          "warehouse": "WH_XS"
        }
      },
      "delivery_analytics": {
        "semantic_view": "ANALYTICS.MART.DELIVERY_ANALYTICS",
        "execution_environment": {
          "type": "warehouse",
          "warehouse": "WH_XS"
        }
      }
    }
  }
  $$;

-- ============================================================================
-- AGENT 3: PER-FACT (5 granular semantic views)
-- ============================================================================

CREATE OR REPLACE AGENT ANALYTICS.MART.REVOPS_AGENT_PERFACT
  COMMENT = 'Agent 3: Per-Fact approach - granular views per fact table'
  FROM SPECIFICATION $$
  {
    "models": {
      "orchestration": "claude-3-5-sonnet"
    },
    "instructions": {
      "orchestration": "Select the most appropriate view: opportunities for pipeline/deals, line_items for products, time_entries for hours/utilization, project_status for project health, deal_to_delivery for cross-system profitability.",
      "response": "You are a Revenue Operations analyst with specialized data views. Match queries to the right data source."
    },
    "tools": [
      {
        "tool_spec": {
          "type": "cortex_analyst_text_to_sql",
          "name": "opportunities",
          "description": "Sales opportunities and pipeline data"
        }
      },
      {
        "tool_spec": {
          "type": "cortex_analyst_text_to_sql",
          "name": "line_items",
          "description": "Product line items on opportunities"
        }
      },
      {
        "tool_spec": {
          "type": "cortex_analyst_text_to_sql",
          "name": "time_entries",
          "description": "Consultant time tracking and utilization"
        }
      },
      {
        "tool_spec": {
          "type": "cortex_analyst_text_to_sql",
          "name": "project_status",
          "description": "Project health, status, and risk"
        }
      },
      {
        "tool_spec": {
          "type": "cortex_analyst_text_to_sql",
          "name": "deal_to_delivery",
          "description": "Cross-system deal profitability and margins"
        }
      }
    ],
    "tool_resources": {
      "opportunities": {
        "semantic_view": "ANALYTICS.MART.OPPORTUNITIES_VIEW",
        "execution_environment": {"type": "warehouse", "warehouse": "WH_XS"}
      },
      "line_items": {
        "semantic_view": "ANALYTICS.MART.LINE_ITEMS_VIEW",
        "execution_environment": {"type": "warehouse", "warehouse": "WH_XS"}
      },
      "time_entries": {
        "semantic_view": "ANALYTICS.MART.TIME_ENTRIES_VIEW",
        "execution_environment": {"type": "warehouse", "warehouse": "WH_XS"}
      },
      "project_status": {
        "semantic_view": "ANALYTICS.MART.PROJECT_STATUS_VIEW",
        "execution_environment": {"type": "warehouse", "warehouse": "WH_XS"}
      },
      "deal_to_delivery": {
        "semantic_view": "ANALYTICS.MART.DEAL_TO_DELIVERY_VIEW",
        "execution_environment": {"type": "warehouse", "warehouse": "WH_XS"}
      }
    }
  }
  $$;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

SHOW AGENTS IN SCHEMA ANALYTICS.MART;

SELECT 'SUCCESS: 3 Cortex Agents created for A/B testing' AS status;
