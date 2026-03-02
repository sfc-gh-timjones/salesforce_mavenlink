select
    id as time_entry_id,
    workspace_id,
    story_id,
    user_id,
    date_performed,
    time_in_minutes,
    time_in_minutes / 60.0 as time_in_hours,
    billable as is_billable,
    approved as is_approved,
    is_invoiced,
    rate_in_cents / 100.0 as rate_dollars,
    cost_rate_in_cents / 100.0 as cost_rate_dollars,
    (time_in_minutes / 60.0) * (rate_in_cents / 100.0) as revenue_dollars,
    (time_in_minutes / 60.0) * (cost_rate_in_cents / 100.0) as cost_dollars,
    currency,
    notes,
    created_at as created_date
from {{ source('mavenlink', 'time_entry') }}
where not _fivetran_deleted
