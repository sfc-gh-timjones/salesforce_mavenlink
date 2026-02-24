with source as (
    select * from {{ source('mavenlink', 'STORY') }}
    where coalesce(DELETED_AT, '9999-12-31'::timestamp) > current_timestamp()
),

renamed as (
    select
        ID                                                  as story_id,
        WORKSPACE_ID                                        as workspace_id,
        CREATOR_ID                                          as creator_id,
        PARENT_ID                                           as parent_story_id,
        ROOT_ID                                             as root_story_id,
        TITLE                                               as title,
        DESCRIPTION                                         as description,
        lower(STORY_TYPE)                                   as story_type,
        lower(STATE)                                        as state,
        PRIORITY                                            as priority,
        POSITION                                            as position,
        ARCHIVED                                            as is_archived,
        PERCENTAGE_COMPLETE                                 as percentage_complete,
        START_DATE                                          as start_date,
        DUE_DATE                                            as due_date,
        SUB_STORY_COUNT                                     as sub_story_count,
        WEIGHT                                              as weight,
        BILLABLE                                            as is_billable,
        FIXED_FEE                                           as is_fixed_fee,
        {{ cents_to_dollars('BUDGET_ESTIMATE_IN_CENTS') }}  as budget_estimate_dollars,
        {{ cents_to_dollars('BUDGET_USED_IN_CENTS') }}      as budget_used_dollars,
        TIME_ESTIMATE_IN_MINUTES                            as time_estimate_minutes,
        LOGGED_BILLABLE_TIME_IN_MINUTES                     as logged_billable_minutes,
        LOGGED_NONBILLABLE_TIME_IN_MINUTES                  as logged_nonbillable_minutes,
        TIME_TRACKABLE                                      as is_time_trackable,
        CREATED_AT                                          as created_at,
        UPDATED_AT                                          as updated_at,
        _FIVETRAN_SYNCED                                    as _fivetran_synced
    from source
)

select * from renamed
