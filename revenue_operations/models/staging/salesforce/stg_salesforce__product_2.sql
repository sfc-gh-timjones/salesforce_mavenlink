with source as (
    select * from {{ source('salesforce', 'PRODUCT_2') }}
    where not coalesce(_FIVETRAN_DELETED, false)
),

renamed as (
    select
        ID                          as product_id,
        NAME                        as product_name,
        PRODUCT_CODE                as product_code,
        FAMILY                      as product_family,
        DESCRIPTION                 as description,
        IS_ACTIVE                   as is_active,
        IS_DELETED                  as is_deleted,
        CURRENCY_ISO_CODE           as currency_code,
        CREATED_DATE                as created_at,
        LAST_MODIFIED_DATE          as updated_at,
        _FIVETRAN_SYNCED            as _fivetran_synced
    from source
)

select * from renamed
