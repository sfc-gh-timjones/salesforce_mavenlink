with opportunities as (
    select * from {{ ref('stg_salesforce__opportunity') }}
    where not is_deleted
),

accounts as (
    select account_id, account_name, industry
    from {{ ref('stg_salesforce__account') }}
),

users as (
    select user_id, full_name, email
    from {{ ref('stg_salesforce__user') }}
),

record_types as (
    select record_type_id, record_type_name
    from {{ ref('stg_salesforce__record_type') }}
),

enhanced as (
    select
        o.opportunity_id,
        o.opportunity_name,
        o.account_id,
        a.account_name,
        a.industry                      as account_industry,
        o.owner_id,
        u.full_name                     as owner_name,
        u.email                         as owner_email,
        o.stage_name,
        o.amount,
        o.probability,
        o.close_date,
        o.opportunity_type,
        o.lead_source,
        o.forecast_category,
        o.is_closed,
        o.is_won,
        o.has_line_items,
        o.currency_code,
        o.push_count,
        o.fiscal_quarter,
        o.fiscal_year,
        rt.record_type_name,
        o.created_at,
        o.updated_at,
        o.last_activity_date,
        datediff('day', o.created_at::date, o.close_date) as days_to_close,
        case
            when o.is_won then 'Won'
            when o.is_closed and not o.is_won then 'Lost'
            else 'Open'
        end as deal_status

    from opportunities o
    left join accounts a on o.account_id = a.account_id
    left join users u on o.owner_id = u.user_id
    left join record_types rt on o.record_type_id = rt.record_type_id
)

select * from enhanced
