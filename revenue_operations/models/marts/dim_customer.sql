with accounts as (
    select * from {{ ref('stg_salesforce__account') }}
    where not is_deleted
),

opp_summary as (
    select * from {{ ref('int_salesforce__account_opportunity_summary') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['a.account_id']) }} as customer_key,
        a.account_id,
        a.account_name,
        a.parent_account_id,
        a.industry,
        a.annual_revenue,
        a.number_of_employees,
        a.billing_city,
        a.billing_state,
        a.billing_country,
        a.phone,
        a.website,
        a.owner_id,
        a.currency_code,
        a.last_activity_date,
        a.created_at,

        coalesce(o.total_opportunities, 0)      as total_opportunities,
        coalesce(o.won_opportunities, 0)        as won_opportunities,
        coalesce(o.open_opportunities, 0)       as open_opportunities,
        coalesce(o.total_won_revenue, 0)        as total_won_revenue,
        coalesce(o.open_pipeline_value, 0)      as open_pipeline_value,
        coalesce(o.avg_deal_size, 0)            as avg_deal_size,
        o.win_rate_pct,
        o.first_won_date,
        o.last_won_date,
        o.avg_days_to_close

    from accounts a
    left join opp_summary o on a.account_id = o.account_id
)

select * from final
