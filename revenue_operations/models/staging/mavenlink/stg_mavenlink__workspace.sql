with source as (
    select * from {{ source('mavenlink', 'WORKSPACE') }}
    where not coalesce(_FIVETRAN_DELETED, false)
),

renamed as (
    select
        ID                                              as workspace_id,
        TITLE                                           as title,
        DESCRIPTION                                     as description,
        case
            when STATUS_KEY = 0 or lower(STATUS_KEY::text) = 'active'    then 'active'
            when STATUS_KEY = 1 or lower(STATUS_KEY::text) = 'archived'  then 'archived'
            when STATUS_KEY = 2 or lower(STATUS_KEY::text) = 'completed' then 'completed'
            when STATUS_KEY = 3 or lower(STATUS_KEY::text) = 'cancelled' then 'cancelled'
            else lower(STATUS_KEY::text)
        end                                             as status_key,
        STATUS_MESSAGE                                  as status_message,
        STATUS_COLOR                                    as status_color,
        CREATOR_ID                                      as creator_id,
        PRIMARY_WORKSPACE_GROUP_ID                      as workspace_group_id,
        ACCESS_LEVEL                                    as access_level,
        ARCHIVED                                        as is_archived,
        BUDGETED                                        as is_budgeted,
        PERCENTAGE_COMPLETE                             as percentage_complete,
        START_DATE                                      as start_date,
        DUE_DATE                                        as due_date,
        EFFECTIVE_DUE_DATE                              as effective_due_date,
        {{ cents_to_dollars('PRICE_IN_CENTS') }}        as budget_dollars,
        {{ cents_to_dollars('BUDGET_USED_IN_CENTS') }}  as budget_used_dollars,
        {{ cents_to_dollars('TOTAL_EXPENSES_IN_CENTS') }} as total_expenses_dollars,
        TARGET_MARGIN                                   as target_margin,
        CURRENCY                                        as currency_code,
        OVER_BUDGET                                     as is_over_budget,
        CUSTOM_OPPORTUNITY_ID_FROM_SALESFORCE_           as custom_opportunity_id_from_salesforce,
        CUSTOM_PGID_000                                 as custom_pgid,
        CUSTOM_INTACCT_CUSTOMER_ID                      as custom_intacct_customer_id,
        CUSTOM_GUP_GLOBAL_ID                            as custom_global_id,
        CREATED_AT                                      as created_at,
        UPDATED_AT                                      as updated_at,
        _FIVETRAN_SYNCED                                as _fivetran_synced
    from source
)

select * from renamed
