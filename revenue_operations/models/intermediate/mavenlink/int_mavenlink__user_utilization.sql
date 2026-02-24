with time_entries as (
    select * from {{ ref('stg_mavenlink__time_entry') }}
),

users as (
    select * from {{ ref('stg_mavenlink__user') }}
),

memberships as (
    select
        user_id,
        billability_target,
        bill_rate_dollars,
        cost_rate_dollars,
        row_number() over (partition by user_id order by bill_rate_dollars desc nulls last) as rn
    from {{ ref('stg_mavenlink__account_membership') }}
),

memberships_deduped as (
    select * from memberships where rn = 1
),

utilization as (
    select
        te.user_id,
        u.full_name,
        u.email_address,

        count(distinct te.workspace_id)                                 as active_project_count,
        count(distinct te.date_performed)                               as days_worked,
        sum(te.time_in_minutes)                                         as total_minutes,
        round(sum(te.time_in_minutes) / 60.0, 2)                       as total_hours,
        sum(case when te.is_billable then te.time_in_minutes else 0 end) as billable_minutes,
        round(sum(case when te.is_billable then te.time_in_minutes else 0 end) / 60.0, 2) as billable_hours,

        case
            when sum(te.time_in_minutes) > 0
            then round(sum(case when te.is_billable then te.time_in_minutes else 0 end)::float /
                        sum(te.time_in_minutes) * 100, 2)
            else 0
        end as utilization_pct,

        m.billability_target,
        m.bill_rate_dollars,
        m.cost_rate_dollars,

        min(te.date_performed)  as first_entry_date,
        max(te.date_performed)  as last_entry_date

    from time_entries te
    inner join users u on te.user_id = u.user_id
    left join memberships_deduped m on te.user_id = m.user_id
    group by te.user_id, u.full_name, u.email_address,
             m.billability_target, m.bill_rate_dollars, m.cost_rate_dollars
)

select * from utilization
