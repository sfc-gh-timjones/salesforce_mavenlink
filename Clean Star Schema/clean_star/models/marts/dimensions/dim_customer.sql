with accounts as (
    select * from {{ ref('stg_salesforce__account') }}
),

opportunities as (
    select * from {{ ref('stg_salesforce__opportunity') }}
),

opp_stats as (
    select
        account_id,
        count(*) as total_opportunities,
        count(case when is_won then 1 end) as won_opportunities,
        count(case when is_closed and not is_won then 1 end) as lost_opportunities,
        count(case when not is_closed then 1 end) as open_opportunities,
        sum(case when is_won then amount else 0 end) as total_won_revenue,
        sum(case when not is_closed then amount else 0 end) as open_pipeline_value,
        avg(case when is_won then amount end) as avg_won_deal_size,
        round(
            100.0 * count(case when is_won then 1 end) / 
            nullif(count(case when is_closed then 1 end), 0),
        2) as win_rate_pct
    from opportunities
    group by account_id
)

select
    a.account_id,
    a.account_name,
    a.industry,
    a.annual_revenue,
    a.number_of_employees,
    a.billing_city,
    a.billing_state,
    a.billing_country,
    a.billing_postal_code,
    a.phone,
    a.website,
    a.owner_id,
    a.last_activity_date,
    a.created_date,
    
    coalesce(os.total_opportunities, 0) as total_opportunities,
    coalesce(os.won_opportunities, 0) as won_opportunities,
    coalesce(os.lost_opportunities, 0) as lost_opportunities,
    coalesce(os.open_opportunities, 0) as open_opportunities,
    coalesce(os.total_won_revenue, 0) as total_won_revenue,
    coalesce(os.open_pipeline_value, 0) as open_pipeline_value,
    os.avg_won_deal_size,
    os.win_rate_pct
    
from accounts a
left join opp_stats os on a.account_id = os.account_id
