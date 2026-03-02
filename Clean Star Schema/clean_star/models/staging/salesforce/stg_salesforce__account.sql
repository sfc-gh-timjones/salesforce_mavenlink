select
    id as account_id,
    name as account_name,
    parent_id as parent_account_id,
    industry,
    annual_revenue,
    number_of_employees,
    billing_city,
    billing_state,
    billing_country,
    billing_postal_code,
    phone,
    website,
    owner_id,
    created_date,
    last_activity_date,
    is_deleted
from {{ source('salesforce', 'account') }}
where not is_deleted
