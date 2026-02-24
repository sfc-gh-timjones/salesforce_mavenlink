with line_items as (
    select * from {{ ref('stg_salesforce__opportunity_line_item') }}
    where not is_deleted
),

products as (
    select product_id, product_name, product_family
    from {{ ref('stg_salesforce__product_2') }}
),

agg as (
    select
        li.opportunity_id,
        count(*)                                    as line_item_count,
        sum(li.total_price)                         as total_line_item_value,
        avg(li.unit_price)                          as avg_unit_price,
        sum(li.quantity)                             as total_quantity,
        count(distinct li.product_id)               as distinct_product_count,
        listagg(distinct p.product_family, ', ')
            within group (order by p.product_family) as product_families,
        min(li.created_at)                          as first_line_item_at,
        max(li.created_at)                          as last_line_item_at

    from line_items li
    left join products p on li.product_id = p.product_id
    group by li.opportunity_id
)

select * from agg
