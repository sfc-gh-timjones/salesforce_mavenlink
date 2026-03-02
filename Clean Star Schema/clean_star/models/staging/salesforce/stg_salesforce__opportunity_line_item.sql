select
    id as line_item_id,
    opportunity_id,
    product_2_id as product_id,
    name as line_item_name,
    product_code,
    quantity,
    unit_price,
    list_price,
    total_price,
    discount as discount_percent,
    service_date,
    currency_iso_code,
    created_date,
    is_deleted
from {{ source('salesforce', 'opportunity_line_item') }}
where not is_deleted
