with stories as (
    select * from {{ ref('stg_mavenlink__story') }}
)

select
    workspace_id,
    
    count(*) as total_stories,
    count(case when story_state = 'completed' then 1 end) as completed_stories,
    count(case when story_state = 'in progress' then 1 end) as in_progress_stories,
    count(case when story_state = 'not started' then 1 end) as not_started_stories,
    
    round(
        100.0 * count(case when story_state = 'completed' then 1 end) / 
        nullif(count(*), 0), 
    2) as story_completion_pct,
    
    round(sum(time_estimate_hours), 2) as total_estimated_hours,
    round(sum(logged_billable_hours), 2) as total_logged_billable_hours,
    round(sum(logged_nonbillable_hours), 2) as total_logged_nonbillable_hours,
    
    round(sum(budget_estimate_dollars), 2) as total_budget_estimate,
    
    count(distinct story_type) as unique_story_types,
    
    min(start_date) as earliest_story_start,
    max(due_date) as latest_story_due
    
from stories
group by workspace_id
