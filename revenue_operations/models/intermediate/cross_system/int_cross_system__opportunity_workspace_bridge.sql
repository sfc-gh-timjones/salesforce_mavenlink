{{
    config(
        materialized='table'
    )
}}

with opportunities as (
    select * from {{ ref('stg_salesforce__opportunity') }}
    where not is_deleted
),

workspaces as (
    select * from {{ ref('stg_mavenlink__workspace') }}
),

bridge as (
    select
        o.opportunity_id,
        o.account_id,
        o.opportunity_name,
        o.owner_id              as opportunity_owner_id,
        o.amount                as opportunity_amount,
        o.stage_name,
        o.is_won,
        o.is_closed,
        o.close_date,
        o.forecast_category,
        o.currency_code,

        w.workspace_id,
        w.title                 as project_title,
        w.status_key            as project_status,
        w.start_date            as project_start_date,
        w.due_date              as project_due_date,
        w.percentage_complete,
        w.budget_dollars        as project_budget,
        w.budget_used_dollars   as project_spend,
        w.is_budgeted,

        case
            when w.workspace_id is not null then true
            else false
        end as has_linked_project,

        case
            when o.is_won and w.workspace_id is not null then 'Deal + Project'
            when o.is_won and w.workspace_id is null then 'Deal Only'
            when not o.is_closed and w.workspace_id is not null then 'Pre-Close Project'
            else 'Unlinked'
        end as link_category

    from opportunities o
    left join workspaces w
        on w.custom_opportunity_id_from_salesforce = o.opportunity_id
)

select * from bridge
