with bridge as (
    select * from {{ ref('int_cross_system__opportunity_workspace_bridge') }}
    where has_linked_project
),

time_summary as (
    select * from {{ ref('int_mavenlink__workspace_time_summary') }}
),

story_comp as (
    select * from {{ ref('int_mavenlink__story_completion') }}
),

accounts as (
    select account_id, account_name, industry
    from {{ ref('stg_salesforce__account') }}
),

opp_owners as (
    select user_id, full_name
    from {{ ref('stg_salesforce__user') }}
),

final as (
    select
        b.opportunity_id,
        b.workspace_id,
        b.account_id,
        a.account_name,
        a.industry,

        b.opportunity_name                      as deal_name,
        b.opportunity_amount                    as deal_amount,
        b.stage_name                            as deal_stage,
        b.is_won                                as deal_is_won,
        b.close_date                            as deal_close_date,
        b.forecast_category,
        b.currency_code,

        ow.full_name                            as sales_rep_name,

        b.project_title                         as project_name,
        b.project_status,
        b.project_start_date,
        b.project_due_date,
        b.percentage_complete                   as project_pct_complete,
        b.project_budget,
        b.project_spend,

        ts.total_hours                          as project_total_hours,
        ts.billable_hours                       as project_billable_hours,
        ts.total_billable_revenue               as project_revenue,
        ts.total_cost                           as project_cost,
        ts.unique_contributors                  as project_team_size,
        ts.billability_pct                      as project_billability_pct,

        sc.total_stories,
        sc.completed_stories,
        sc.completion_rate_pct                  as task_completion_rate,

        case
            when ts.total_billable_revenue > 0 and b.opportunity_amount > 0
            then round(ts.total_billable_revenue / b.opportunity_amount * 100, 2)
            else null
        end as revenue_realization_pct,

        case
            when b.project_budget > 0
            then round((b.project_budget - coalesce(b.project_spend, 0)) / b.project_budget * 100, 2)
            else null
        end as margin_pct,

        datediff('day', b.close_date, b.project_start_date) as days_deal_to_kickoff,
        datediff('day', b.project_start_date, b.project_due_date) as planned_project_duration_days

    from bridge b
    left join time_summary ts on b.workspace_id = ts.workspace_id
    left join story_comp sc on b.workspace_id = sc.workspace_id
    left join accounts a on b.account_id = a.account_id
    left join opp_owners ow on b.opportunity_owner_id = ow.user_id
)

select * from final
