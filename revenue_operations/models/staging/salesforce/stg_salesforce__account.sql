with source as (
    select * from {{ source('salesforce', 'ACCOUNT') }}
    where not coalesce(_FIVETRAN_DELETED, false)
),

renamed as (
    select
        ID                          as account_id,
        NAME                        as account_name,
        PARENT_ID                   as parent_account_id,
        OWNER_ID                    as owner_id,
        INDUSTRY                    as industry,
        ANNUAL_REVENUE              as annual_revenue,
        NUMBER_OF_EMPLOYEES         as number_of_employees,
        BILLING_STREET              as billing_street,
        BILLING_CITY                as billing_city,
        BILLING_STATE               as billing_state,
        BILLING_POSTAL_CODE         as billing_postal_code,
        BILLING_COUNTRY             as billing_country,
        PHONE                       as phone,
        WEBSITE                     as website,
        DESCRIPTION                 as description,
        ACCOUNT_SOURCE              as account_source,
        CURRENCY_ISO_CODE           as currency_code,
        IS_DELETED                  as is_deleted,
        CREATED_DATE                as created_at,
        LAST_MODIFIED_DATE          as updated_at,
        LAST_ACTIVITY_DATE          as last_activity_date,
        _FIVETRAN_SYNCED            as _fivetran_synced
    from source
)

select * from renamed
