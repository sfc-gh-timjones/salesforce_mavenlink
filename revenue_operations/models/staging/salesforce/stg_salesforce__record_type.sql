with source as (
    select * from {{ source('salesforce', 'RECORD_TYPE') }}
),

renamed as (
    select
        ID                          as record_type_id,
        NAME                        as record_type_name,
        DEVELOPER_NAME              as developer_name,
        SOBJECT_TYPE                as sobject_type,
        IS_ACTIVE                   as is_active,
        CREATED_DATE                as created_at,
        LAST_MODIFIED_DATE          as updated_at,
        _FIVETRAN_SYNCED            as _fivetran_synced
    from source
)

select * from renamed
