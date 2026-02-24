with line_items as (
    select * from {{ ref('stg_salesforce__opportunity_line_item') }}
    where not is_deleted
),

opportunities as (
    select
        opportunity_id,
        account_id,
        stage_name,
        is_won,
        is_closed,
        close_date
    from {{ ref('stg_salesforce__opportunity') }}
    where not is_deleted
),

products as (
    select product_id, product_name, product_family
    from {{ ref('stg_salesforce__product_2') }}
),

final as (
    select
        li.opportunity_line_item_id,
        li.opportunity_id,
        o.account_id,
        li.product_id,
        li.pricebook_entry_id,
        li.line_item_name,
        p.product_name,
        p.product_family,
        li.quantity,
        li.unit_price,
        li.list_price,
        li.total_price,
        li.discount_percent,
        li.currency_code,
        li.service_date,
        o.stage_name                as opportunity_stage,
        o.is_won                    as opportunity_is_won,
        o.is_closed                 as opportunity_is_closed,
        o.close_date                as opportunity_close_date,
        li.created_at

    from line_items li
    inner join opportunities o on li.opportunity_id = o.opportunity_id
    left join products p on li.product_id = p.product_id
)

select * from final
