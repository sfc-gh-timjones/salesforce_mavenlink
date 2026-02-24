with source as (
    select * from {{ source('mavenlink', 'ACCOUNT_MEMBERSHIP') }}
    where not coalesce(_FIVETRAN_DELETED, false)
),

renamed as (
    select
        ID                                              as account_membership_id,
        USER_ID                                         as user_id,
        MANAGER_ID                                      as manager_id,
        DEFAULT_ROLE_ID                                 as default_role_id,
        PERMISSION                                      as permission_level,
        IS_ADMINISTRATOR                                as is_administrator,
        IS_PROJECT_LEAD                                 as is_project_lead,
        IS_PUNCH_CLOCK_USER                             as is_punch_clock_user,
        CAN_CREATE_WORKSPACE                            as can_create_workspace,
        CAN_VIEW_REPORTS                                as can_view_reports,
        IS_OWNER                                        as is_owner,
        {{ cents_to_dollars('BILL_RATE_IN_CENTS') }}    as bill_rate_dollars,
        {{ cents_to_dollars('COST_RATE_IN_CENTS') }}    as cost_rate_dollars,
        BILLABILITY_TARGET                              as billability_target,
        DISABLED_AT                                     as disabled_at,
        CREATED_AT                                      as created_at,
        UPDATED_AT                                      as updated_at,
        _FIVETRAN_SYNCED                                as _fivetran_synced
    from source
)

select * from renamed
