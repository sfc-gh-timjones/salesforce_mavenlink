select
    o.id as opportunity_id,
    o.account_id,
    o.owner_id,
    o.name as opportunity_name,
    o.stage_name,
    o.amount,
    o.probability,
    o.close_date,
    o.type as opportunity_type,
    o.lead_source,
    o.forecast_category_name as forecast_category,
    o.is_closed,
    o.is_won,
    o.currency_iso_code,
    o.fiscal_quarter,
    o.fiscal_year,
    o.push_count,
    o.last_activity_date,
    o.created_date,
    o.is_deleted,
    rt.name as record_type_name,
    rt.sobject_type as record_type_object,
    o.linked_booking_opportunity_c as linked_booking_opportunity_id
from {{ source('salesforce', 'opportunity') }} o
inner join {{ source('salesforce', 'record_type') }} rt
    on o.record_type_id = rt.id
    and rt.name = 'Booking'
where not o.is_deleted
