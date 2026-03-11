with opportunities as (
    select * from {{ ref('stg_salesforce__opportunity') }}
),

accounts as (
    select * from {{ ref('stg_salesforce__account') }}
),

users as (
    select * from {{ ref('stg_salesforce__user') }}
),

workspaces as (
    select * from {{ ref('stg_mavenlink__workspace') }}
),

line_item_summary as (
    select * from {{ ref('int_opportunity_line_item_summary') }}
),

time_summary as (
    select * from {{ ref('int_workspace_time_summary') }}
),

story_summary as (
    select * from {{ ref('int_workspace_story_summary') }}
),

opp_with_workspace as (
    select
        o.*,
        w.workspace_id,
        w.project_name,
        w.project_status,
        w.start_date as project_start_date,
        w.due_date as project_due_date,
        w.percentage_complete as project_pct_complete,
        w.budget_dollars as project_budget,
        w.budget_used_dollars as project_spend,
        w.is_over_budget as project_is_over_budget,
        w.deal_close_date as ml_deal_close_date
    from opportunities o
    left join workspaces w on o.opportunity_id = w.linked_opportunity_id
)

select
    opp.opportunity_id,
    opp.workspace_id,
    opp.account_id,
    opp.owner_id as sales_rep_id,
    lis.primary_product_id,
    
    opp.opportunity_name,
    opp.stage_name,
    opp.opportunity_type,
    opp.lead_source,
    opp.forecast_category,
    opp.is_closed,
    opp.is_won,
    opp.currency_iso_code,
    opp.fiscal_quarter,
    opp.fiscal_year,
    opp.record_type_name,
    opp.record_type_object,
    opp.linked_booking_opportunity_id,
    
    a.account_name,
    a.industry,
    u.full_name as sales_rep_name,
    
    opp.amount as deal_amount,
    opp.probability,
    opp.push_count,
    opp.close_date as deal_close_date,
    opp.created_date as deal_created_date,
    opp.last_activity_date as deal_last_activity_date,
    
    coalesce(lis.line_item_count, 0) as line_item_count,
    coalesce(lis.unique_products, 0) as unique_products,
    coalesce(lis.unique_product_families, 0) as unique_product_families,
    coalesce(lis.total_line_item_value, 0) as total_line_item_value,
    lis.avg_unit_price,
    lis.avg_discount_pct,
    lis.primary_product_name,
    lis.primary_product_family,
    lis.primary_product_value,
    lis.all_product_families,
    
    opp.project_name,
    opp.project_status,
    opp.project_start_date,
    opp.project_due_date,
    opp.project_pct_complete,
    opp.project_budget,
    opp.project_spend,
    opp.project_is_over_budget,
    case when opp.workspace_id is not null then true else false end as has_linked_project,
    
    coalesce(ts.total_hours, 0) as project_total_hours,
    coalesce(ts.billable_hours, 0) as project_billable_hours,
    coalesce(ts.non_billable_hours, 0) as project_non_billable_hours,
    coalesce(ts.total_revenue, 0) as project_revenue,
    coalesce(ts.total_cost, 0) as project_cost,
    coalesce(ts.gross_margin, 0) as project_gross_margin,
    ts.billability_pct as project_billability_pct,
    ts.avg_bill_rate as project_avg_bill_rate,
    ts.avg_cost_rate as project_avg_cost_rate,
    coalesce(ts.unique_contributors, 0) as project_team_size,
    ts.first_time_entry_date as project_first_time_entry,
    ts.last_time_entry_date as project_last_time_entry,
    ts.span_days as project_active_span_days,
    ts.top_contributor_name as project_lead_name,
    ts.top_contributor_title as project_lead_title,
    ts.top_contributor_hours as project_lead_hours,
    
    coalesce(ss.total_stories, 0) as project_total_tasks,
    coalesce(ss.completed_stories, 0) as project_completed_tasks,
    coalesce(ss.in_progress_stories, 0) as project_in_progress_tasks,
    ss.story_completion_pct as project_task_completion_pct,
    ss.total_estimated_hours as project_estimated_hours,
    
    round(
        100.0 * coalesce(ts.total_revenue, 0) / nullif(opp.amount, 0),
    2) as revenue_realization_pct,
    
    round(
        100.0 * coalesce(ts.gross_margin, 0) / nullif(ts.total_revenue, 0),
    2) as margin_pct,
    
    datediff('day', opp.close_date, opp.project_start_date) as days_close_to_kickoff,
    datediff('day', opp.project_start_date, opp.project_due_date) as planned_project_duration_days,
    
    case
        when not opp.is_closed then 'Open'
        when opp.is_won and opp.project_status = 'completed' then 'Delivered'
        when opp.is_won and opp.project_status = 'active' then 'In Delivery'
        when opp.is_won and opp.project_status is null then 'Won - No Project'
        when opp.is_won then 'Won - ' || coalesce(opp.project_status, 'Unknown')
        else 'Lost'
    end as deal_delivery_status,
    
    case
        when opp.project_status in ('completed', 'cancelled', 'archived') then opp.project_status
        when opp.project_spend > opp.project_budget then 'At Risk - Over Budget'
        when opp.project_due_date < current_date() and opp.project_status = 'active' then 'At Risk - Overdue'
        when ts.billability_pct < 50 and ts.total_hours > 10 then 'At Risk - Low Billability'
        when round(100.0 * coalesce(ts.gross_margin, 0) / nullif(ts.total_revenue, 0), 2) < 20 
            and ts.total_revenue > 0 then 'At Risk - Low Margin'
        when opp.project_pct_complete > 0 then 'On Track'
        when opp.workspace_id is null then 'No Project'
        else 'Not Started'
    end as project_health_status

from opp_with_workspace opp
left join accounts a on opp.account_id = a.account_id
left join users u on opp.owner_id = u.sf_user_id
left join line_item_summary lis on opp.opportunity_id = lis.opportunity_id
left join time_summary ts on opp.workspace_id = ts.workspace_id
left join story_summary ss on opp.workspace_id = ss.workspace_id
