with products as (
    select * from {{ ref('stg_salesforce__product') }}
),

line_item_stats as (
    select
        product_id,
        count(*) as times_sold,
        count(distinct opportunity_id) as unique_opportunities,
        sum(total_price) as total_revenue,
        sum(quantity) as total_quantity_sold,
        avg(unit_price) as avg_selling_price
    from {{ ref('stg_salesforce__opportunity_line_item') }}
    group by product_id
)

select
    p.product_id,
    p.product_name,
    p.product_code,
    p.product_description,
    p.product_family,
    p.is_active,
    p.currency_iso_code,
    p.created_date,
    
    coalesce(lis.times_sold, 0) as times_sold,
    coalesce(lis.unique_opportunities, 0) as unique_opportunities,
    coalesce(lis.total_revenue, 0) as total_revenue,
    coalesce(lis.total_quantity_sold, 0) as total_quantity_sold,
    lis.avg_selling_price
    
from products p
left join line_item_stats lis on p.product_id = lis.product_id
