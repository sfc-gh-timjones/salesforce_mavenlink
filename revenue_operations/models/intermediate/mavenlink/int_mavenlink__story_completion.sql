with stories as (
    select * from {{ ref('stg_mavenlink__story') }}
    where not is_archived
),

completion as (
    select
        workspace_id,

        count(*)                                                            as total_stories,
        count(case when state in ('completed', 'accepted') then 1 end)      as completed_stories,
        count(case when state = 'started' then 1 end)                       as in_progress_stories,
        count(case when state = 'not started' then 1 end)                   as not_started_stories,
        count(case when state = 'rejected' then 1 end)                      as rejected_stories,

        case
            when count(*) > 0
            then round(count(case when state in ('completed', 'accepted') then 1 end)::float /
                        count(*) * 100, 2)
            else 0
        end as completion_rate_pct,

        avg(percentage_complete)                                            as avg_pct_complete,

        count(case when story_type = 'task' then 1 end)                     as task_count,
        count(case when story_type = 'milestone' then 1 end)                as milestone_count,
        count(case when story_type = 'deliverable' then 1 end)              as deliverable_count,

        sum(coalesce(time_estimate_minutes, 0))                             as total_estimated_minutes,
        sum(coalesce(logged_billable_minutes, 0))                           as total_logged_billable_minutes,
        sum(coalesce(logged_nonbillable_minutes, 0))                        as total_logged_nonbillable_minutes

    from stories
    group by workspace_id
)

select * from completion
