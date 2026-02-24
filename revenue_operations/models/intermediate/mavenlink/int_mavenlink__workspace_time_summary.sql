with time_entries as (
    select * from {{ ref('stg_mavenlink__time_entry') }}
),

summary as (
    select
        workspace_id,

        count(*)                                                        as total_time_entries,
        count(distinct user_id)                                         as unique_contributors,
        sum(time_in_minutes)                                            as total_minutes,
        round(sum(time_in_minutes) / 60.0, 2)                          as total_hours,
        sum(case when is_billable then time_in_minutes else 0 end)      as billable_minutes,
        round(sum(case when is_billable then time_in_minutes else 0 end) / 60.0, 2) as billable_hours,
        sum(case when not is_billable then time_in_minutes else 0 end)  as non_billable_minutes,

        case
            when sum(time_in_minutes) > 0
            then round(sum(case when is_billable then time_in_minutes else 0 end)::float /
                        sum(time_in_minutes) * 100, 2)
            else 0
        end                                                             as billability_pct,

        sum(case when is_billable then time_in_hours * rate_dollars else 0 end) as total_billable_revenue,
        sum(time_in_hours * coalesce(cost_rate_dollars, 0))             as total_cost,

        min(date_performed)                                             as first_entry_date,
        max(date_performed)                                             as last_entry_date

    from time_entries
    group by workspace_id
)

select * from summary
