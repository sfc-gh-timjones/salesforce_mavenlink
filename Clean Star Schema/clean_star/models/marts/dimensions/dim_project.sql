with workspaces as (
    select * from {{ ref('stg_mavenlink__workspace') }}
),

time_summary as (
    select * from {{ ref('int_workspace_time_summary') }}
),

story_summary as (
    select * from {{ ref('int_workspace_story_summary') }}
)

select
    w.workspace_id,
    w.project_name,
    w.project_description,
    w.project_status,
    w.status_message,
    w.start_date,
    w.due_date,
    w.effective_due_date,
    w.percentage_complete,
    w.is_budgeted,
    w.budget_dollars,
    w.budget_used_dollars,
    w.budget_dollars - coalesce(w.budget_used_dollars, 0) as budget_remaining_dollars,
    w.is_over_budget,
    w.is_archived,
    w.currency,
    w.linked_opportunity_id,
    w.intacct_customer_id,
    w.sales_rep_name,
    w.mrr_amount,
    w.ps_amount,
    w.deal_close_date,
    w.created_date,
    
    case when w.due_date < current_date() and w.project_status = 'active' then true else false end as is_overdue,
    
    case
        when w.project_status in ('completed', 'cancelled', 'archived') then w.project_status
        when w.budget_used_dollars > w.budget_dollars then 'At Risk - Over Budget'
        when w.due_date < current_date() then 'At Risk - Overdue'
        when coalesce(ts.billability_pct, 0) < 50 and ts.total_hours > 10 then 'At Risk - Low Billability'
        when w.percentage_complete > 0 then 'In Progress'
        else 'Not Started'
    end as health_status,
    
    coalesce(ts.total_hours, 0) as total_hours,
    coalesce(ts.billable_hours, 0) as billable_hours,
    coalesce(ts.non_billable_hours, 0) as non_billable_hours,
    coalesce(ts.total_revenue, 0) as total_billable_revenue,
    coalesce(ts.total_cost, 0) as total_cost,
    coalesce(ts.gross_margin, 0) as gross_margin,
    ts.avg_bill_rate,
    ts.avg_cost_rate,
    ts.billability_pct,
    coalesce(ts.unique_contributors, 0) as unique_contributors,
    ts.first_time_entry_date,
    ts.last_time_entry_date,
    ts.top_contributor_name,
    ts.top_contributor_title,
    ts.top_contributor_hours,
    
    coalesce(ss.total_stories, 0) as total_stories,
    coalesce(ss.completed_stories, 0) as completed_stories,
    coalesce(ss.in_progress_stories, 0) as in_progress_stories,
    ss.story_completion_pct,
    ss.total_estimated_hours
    
from workspaces w
left join time_summary ts on w.workspace_id = ts.workspace_id
left join story_summary ss on w.workspace_id = ss.workspace_id
