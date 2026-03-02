with line_items as (
    select * from {{ ref('stg_salesforce__opportunity_line_item') }}
),

products as (
    select * from {{ ref('stg_salesforce__product') }}
),

line_items_enriched as (
    select
        li.*,
        p.product_name,
        p.product_family,
        p.is_active as product_is_active
    from line_items li
    left join products p on li.product_id = p.product_id
),

ranked_products as (
    select
        opportunity_id,
        product_id,
        product_name,
        product_family,
        total_price,
        row_number() over (
            partition by opportunity_id 
            order by total_price desc
        ) as rank_by_value
    from line_items_enriched
)

select
    li.opportunity_id,
    
    count(distinct li.line_item_id) as line_item_count,
    count(distinct li.product_id) as unique_products,
    count(distinct li.product_family) as unique_product_families,
    
    round(sum(li.total_price), 2) as total_line_item_value,
    round(sum(li.quantity), 2) as total_quantity,
    round(avg(li.unit_price), 2) as avg_unit_price,
    round(avg(li.discount_percent), 2) as avg_discount_pct,
    
    round(max(li.total_price), 2) as largest_line_item_value,
    round(min(li.total_price), 2) as smallest_line_item_value,
    
    max(case when rp.rank_by_value = 1 then rp.product_id end) as primary_product_id,
    max(case when rp.rank_by_value = 1 then rp.product_name end) as primary_product_name,
    max(case when rp.rank_by_value = 1 then rp.product_family end) as primary_product_family,
    max(case when rp.rank_by_value = 1 then rp.total_price end) as primary_product_value,
    
    listagg(distinct li.product_family, ', ') within group (order by li.product_family) as all_product_families,
    
    round(
        100.0 * max(case when rp.rank_by_value = 1 then rp.total_price end) / 
        nullif(sum(li.total_price), 0), 
    2) as primary_product_value_pct

from line_items_enriched li
left join ranked_products rp 
    on li.opportunity_id = rp.opportunity_id 
    and li.product_id = rp.product_id
group by li.opportunity_id
