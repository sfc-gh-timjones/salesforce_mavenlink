with workspaces as (
    select * from {{ ref('stg_mavenlink__workspace') }}
),

time_summary as (
    select * from {{ ref('int_mavenlink__workspace_time_summary') }}
),

budget as (
    select
        w.workspace_id,
        w.title,
        w.status_key,
        w.is_budgeted,
        w.budget_dollars,
        w.budget_used_dollars,
        w.total_expenses_dollars,
        w.percentage_complete,
        w.target_margin,
        w.start_date,
        w.due_date,

        t.total_hours,
        t.billable_hours,
        t.total_billable_revenue,
        t.total_cost,
        t.unique_contributors,

        case
            when w.budget_dollars > 0
            then round(w.budget_used_dollars / w.budget_dollars * 100, 2)
            else null
        end as budget_consumed_pct,

        w.budget_dollars - coalesce(w.budget_used_dollars, 0) as budget_remaining_dollars,

        case
            when w.budget_dollars > 0 and w.percentage_complete > 0
            then round((w.budget_used_dollars / w.budget_dollars) /
                        (w.percentage_complete / 100.0), 2)
            else null
        end as burn_rate_index,

        case
            when w.due_date is not null and w.due_date < current_date() and w.status_key = 'active'
            then true
            else false
        end as is_overdue

    from workspaces w
    left join time_summary t on w.workspace_id = t.workspace_id
)

select * from budget
