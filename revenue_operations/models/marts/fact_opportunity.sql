with opp_enhanced as (
    select * from {{ ref('int_salesforce__opportunity_enhanced') }}
),

line_items_agg as (
    select * from {{ ref('int_salesforce__opportunity_line_items_agg') }}
),

bridge as (
    select
        opportunity_id,
        workspace_id,
        project_title,
        project_status,
        project_budget,
        project_spend,
        has_linked_project,
        link_category
    from {{ ref('int_cross_system__opportunity_workspace_bridge') }}
),

final as (
    select
        o.opportunity_id,
        o.account_id,
        o.owner_id,
        o.opportunity_name,
        o.account_name,
        o.account_industry,
        o.owner_name,
        o.stage_name,
        o.deal_status,
        o.amount,
        o.probability,
        o.close_date,
        o.opportunity_type,
        o.lead_source,
        o.forecast_category,
        o.is_closed,
        o.is_won,
        o.currency_code,
        o.days_to_close,
        o.push_count,
        o.fiscal_quarter,
        o.fiscal_year,
        o.created_at,
        o.last_activity_date,

        li.line_item_count,
        li.total_line_item_value,
        li.distinct_product_count,
        li.product_families,

        b.workspace_id              as linked_workspace_id,
        b.project_title             as linked_project_title,
        b.project_status            as linked_project_status,
        b.project_budget            as linked_project_budget,
        b.project_spend             as linked_project_spend,
        b.has_linked_project,
        b.link_category

    from opp_enhanced o
    left join line_items_agg li on o.opportunity_id = li.opportunity_id
    left join bridge b on o.opportunity_id = b.opportunity_id
)

select * from final
