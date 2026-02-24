with opps as (
    select
        account_id,
        amount::number(38,2)        as amount,
        is_won,
        is_closed,
        is_deleted,
        close_date,
        created_at
    from {{ ref('stg_salesforce__opportunity') }}
    where not is_deleted
),

summary as (
    select
        account_id,

        count(*)::number(38,0)                                          as total_opportunities,
        count(case when is_won then 1 end)::number(38,0)                as won_opportunities,
        count(case when is_closed and not is_won then 1 end)::number(38,0) as lost_opportunities,
        count(case when not is_closed then 1 end)::number(38,0)         as open_opportunities,

        coalesce(sum(case when is_won then amount end), 0)::number(38,2)    as total_won_revenue,
        coalesce(sum(case when not is_closed then amount end), 0)::number(38,2) as open_pipeline_value,
        coalesce(avg(case when is_won then amount end), 0)::number(38,2)    as avg_deal_size,

        case
            when count(case when is_closed then 1 end) > 0
            then round(count(case when is_won then 1 end)::float /
                        count(case when is_closed then 1 end) * 100, 2)
            else 0.0
        end                                                             as win_rate_pct,

        min(case when is_won then close_date end)                       as first_won_date,
        max(case when is_won then close_date end)                       as last_won_date,
        coalesce(avg(case when is_closed then
            datediff('day', created_at::date, close_date)::number(38,0) end), 0)::number(38,2) as avg_days_to_close

    from opps
    group by account_id
)

select * from summary
