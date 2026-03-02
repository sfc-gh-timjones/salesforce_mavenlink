select
    id as story_id,
    workspace_id,
    title as story_title,
    story_type,
    state as story_state,
    budget_estimate_in_cents / 100.0 as budget_estimate_dollars,
    time_estimate_in_minutes / 60.0 as time_estimate_hours,
    logged_billable_time_in_minutes / 60.0 as logged_billable_hours,
    logged_nonbillable_time_in_minutes / 60.0 as logged_nonbillable_hours,
    percentage_complete,
    due_date,
    start_date,
    created_at as created_date
from {{ source('mavenlink', 'story') }}
