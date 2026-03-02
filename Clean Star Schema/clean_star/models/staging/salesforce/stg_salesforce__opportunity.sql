select
    id as opportunity_id,
    account_id,
    owner_id,
    name as opportunity_name,
    stage_name,
    amount,
    probability,
    close_date,
    type as opportunity_type,
    lead_source,
    forecast_category_name as forecast_category,
    is_closed,
    is_won,
    currency_iso_code,
    fiscal_quarter,
    fiscal_year,
    push_count,
    last_activity_date,
    created_date,
    is_deleted
from {{ source('salesforce', 'opportunity') }}
where not is_deleted
