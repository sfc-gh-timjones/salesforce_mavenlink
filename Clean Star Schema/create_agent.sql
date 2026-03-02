/*
 * Opportunity Delivery Agent - Creation Script
 * 
 * Creates a Cortex Agent on top of the OPPORTUNITY_DELIVERY_ANALYTICS semantic view.
 * Run AFTER deploy_semantic_view.py has created the semantic view.
 *
 * Semantic View: ANALYTICS2.MART.OPPORTUNITY_DELIVERY_ANALYTICS
 * Agent Location: ANALYTICS2.AGENTS.OPPORTUNITY_DELIVERY_AGENT
 */

USE ROLE ACCOUNTADMIN;
USE DATABASE ANALYTICS2;
USE WAREHOUSE WH_XS;

-- Create agents schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS ANALYTICS2.AGENTS;

-- ============================================================================
-- OPPORTUNITY DELIVERY AGENT
-- ============================================================================

CREATE OR REPLACE AGENT ANALYTICS2.AGENTS.OPPORTUNITY_DELIVERY_AGENT
  COMMENT = 'Agent for opportunity delivery analytics - analyzes deal profitability, project delivery, and cross-system metrics'
  FROM SPECIFICATION $$
  {
    "models": {
      "orchestration": "auto"
    },
    "instructions": {
      "orchestration": "You analyze opportunity delivery data spanning Salesforce CRM and Mavenlink project management. Use the semantic view to answer questions about sales pipeline, deal profitability, project delivery, and cross-system analytics.",
      "response": "You are a Revenue Operations analyst specializing in deal-to-delivery analytics. Format your responses as follows:\n\n1. **Lead with bullet points** - Summarize key insights in clear, concise bullets\n2. **Use bold text** for important metrics, names, and callouts\n3. **Use horizontal bar charts** when comparing metrics across categories (e.g., by rep, product, industry)\n4. **Show raw data after insights** - Include the underlying data table below your bullet point summary\n\nKeep explanations brief. Let the data speak for itself."
    },
    "tools": [
      {
        "tool_spec": {
          "type": "cortex_analyst_text_to_sql",
          "name": "opportunity_delivery",
          "description": "Query opportunity and delivery data - sales pipeline, customers, products, projects, and profitability metrics. Combines Salesforce CRM data with Mavenlink project management data."
        }
      }
    ],
    "tool_resources": {
      "opportunity_delivery": {
        "semantic_view": "ANALYTICS2.MART.OPPORTUNITY_DELIVERY_ANALYTICS",
        "execution_environment": {
          "type": "warehouse",
          "warehouse": "WH_XS"
        }
      }
    }
  }
  $$;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

DESCRIBE AGENT ANALYTICS2.AGENTS.OPPORTUNITY_DELIVERY_AGENT;

SELECT 'SUCCESS: Opportunity Delivery Agent created' AS status;
