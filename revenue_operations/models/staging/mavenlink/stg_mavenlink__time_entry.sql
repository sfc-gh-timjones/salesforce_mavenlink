with source as (
    select * from {{ source('mavenlink', 'TIME_ENTRY') }}
    where not coalesce(_FIVETRAN_DELETED, false)
),

renamed as (
    select
        ID                                          as time_entry_id,
        WORKSPACE_ID                                as workspace_id,
        STORY_ID                                    as story_id,
        USER_ID                                     as user_id,
        TIME_IN_MINUTES                             as time_in_minutes,
        round(TIME_IN_MINUTES / 60.0, 2)            as time_in_hours,
        BILLABLE                                    as is_billable,
        NOTES                                       as notes,
        {{ cents_to_dollars('RATE_IN_CENTS') }}     as rate_dollars,
        {{ cents_to_dollars('COST_RATE_IN_CENTS') }} as cost_rate_dollars,
        CURRENCY                                    as currency_code,
        TAXABLE                                     as is_taxable,
        APPROVED                                    as is_approved,
        IS_INVOICED                                 as is_invoiced,
        DATE_PERFORMED                              as date_performed,
        CREATED_AT                                  as created_at,
        UPDATED_AT                                  as updated_at,
        _FIVETRAN_SYNCED                            as _fivetran_synced
    from source
)

select * from renamed
