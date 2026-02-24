with time_entries as (
    select * from {{ ref('stg_mavenlink__time_entry') }}
),

workspaces as (
    select workspace_id, title as project_title, status_key as project_status
    from {{ ref('stg_mavenlink__workspace') }}
),

stories as (
    select story_id, title as task_title, story_type, state as task_state
    from {{ ref('stg_mavenlink__story') }}
),

users as (
    select user_id, full_name
    from {{ ref('stg_mavenlink__user') }}
),

bridge as (
    select workspace_id, opportunity_id, account_id
    from {{ ref('int_cross_system__opportunity_workspace_bridge') }}
    where has_linked_project
),

final as (
    select
        te.time_entry_id,
        te.workspace_id,
        te.story_id,
        te.user_id,
        te.date_performed,
        te.time_in_minutes,
        te.time_in_hours,
        te.is_billable,
        te.is_approved,
        te.is_invoiced,
        te.rate_dollars,
        te.cost_rate_dollars,
        te.currency_code,
        te.notes,

        case when te.is_billable then te.time_in_hours * coalesce(te.rate_dollars, 0) else 0 end as revenue_dollars,
        te.time_in_hours * coalesce(te.cost_rate_dollars, 0) as cost_dollars,

        w.project_title,
        w.project_status,
        s.task_title,
        s.story_type,
        s.task_state,
        u.full_name                 as user_name,

        br.opportunity_id           as linked_opportunity_id,
        br.account_id               as linked_account_id,

        te.created_at

    from time_entries te
    left join workspaces w on te.workspace_id = w.workspace_id
    left join stories s on te.story_id = s.story_id
    left join users u on te.user_id = u.user_id
    left join bridge br on te.workspace_id = br.workspace_id
)

select * from final
