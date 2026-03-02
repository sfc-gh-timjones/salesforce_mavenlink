select
    id as product_id,
    name as product_name,
    product_code,
    description as product_description,
    family as product_family,
    is_active,
    currency_iso_code,
    created_date,
    is_deleted
from {{ source('salesforce', 'product_2') }}
where not is_deleted
