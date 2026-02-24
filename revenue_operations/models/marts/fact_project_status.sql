with budget as (
    select * from {{ ref('int_mavenlink__workspace_budget_status') }}
),

story_comp as (
    select * from {{ ref('int_mavenlink__story_completion') }}
),

bridge as (
    select
        workspace_id,
        opportunity_id,
        account_id,
        opportunity_amount,
        stage_name,
        is_won
    from {{ ref('int_cross_system__opportunity_workspace_bridge') }}
    where has_linked_project
),

accounts as (
    select account_id, account_name
    from {{ ref('stg_salesforce__account') }}
),

final as (
    select
        b.workspace_id,
        b.title                             as project_name,
        b.status_key                        as project_status,
        b.start_date,
        b.due_date,
        b.percentage_complete,
        b.is_overdue,

        b.budget_dollars,
        b.budget_used_dollars,
        b.budget_remaining_dollars,
        b.budget_consumed_pct,
        b.burn_rate_index,

        b.total_hours,
        b.billable_hours,
        b.total_billable_revenue,
        b.total_cost,
        b.unique_contributors,

        sc.total_stories,
        sc.completed_stories,
        sc.in_progress_stories,
        sc.completion_rate_pct              as story_completion_rate,

        br.opportunity_id                   as linked_opportunity_id,
        br.opportunity_amount               as deal_amount,
        br.account_id                       as linked_account_id,
        a.account_name                      as customer_name,

        case
            when b.budget_consumed_pct > 90 and b.percentage_complete < 75 then 'At Risk - Over Budget'
            when b.is_overdue then 'At Risk - Overdue'
            when b.burn_rate_index > 1.2 then 'At Risk - High Burn'
            when b.status_key = 'active' and b.percentage_complete > 50 then 'On Track'
            when b.status_key = 'completed' then 'Completed'
            when b.status_key = 'cancelled' then 'Cancelled'
            else 'In Progress'
        end as health_status

    from budget b
    left join story_comp sc on b.workspace_id = sc.workspace_id
    left join bridge br on b.workspace_id = br.workspace_id
    left join accounts a on br.account_id = a.account_id
)

select * from final
