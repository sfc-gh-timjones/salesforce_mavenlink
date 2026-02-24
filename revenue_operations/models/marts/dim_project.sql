with budget as (
    select * from {{ ref('int_mavenlink__workspace_budget_status') }}
),

story_completion as (
    select * from {{ ref('int_mavenlink__story_completion') }}
),

bridge as (
    select
        workspace_id,
        opportunity_id,
        account_id,
        has_linked_project
    from {{ ref('int_cross_system__opportunity_workspace_bridge') }}
    where has_linked_project
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['b.workspace_id']) }} as project_key,
        b.workspace_id,
        b.title                         as project_name,
        b.status_key                    as project_status,
        b.is_budgeted,
        b.start_date,
        b.due_date,
        b.percentage_complete,
        b.budget_dollars,
        b.budget_used_dollars,
        b.budget_remaining_dollars,
        b.budget_consumed_pct,
        b.total_hours,
        b.billable_hours,
        b.total_billable_revenue,
        b.total_cost,
        b.unique_contributors,
        b.burn_rate_index,
        b.is_overdue,

        sc.total_stories,
        sc.completed_stories,
        sc.completion_rate_pct          as story_completion_rate,
        sc.task_count,
        sc.milestone_count,

        br.opportunity_id               as linked_opportunity_id,
        br.account_id                   as linked_account_id

    from budget b
    left join story_completion sc on b.workspace_id = sc.workspace_id
    left join bridge br on b.workspace_id = br.workspace_id
)

select * from final
