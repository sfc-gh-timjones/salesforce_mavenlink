with source as (
    select * from {{ source('mavenlink', 'USER') }}
    where not coalesce(_FIVETRAN_DELETED, false)
),

renamed as (
    select
        ID                  as user_id,
        FULL_NAME           as full_name,
        EMAIL_ADDRESS       as email_address,
        HEADLINE            as headline,
        ROLE_ID             as role_id,
        ACCOUNT_ID          as account_id,
        PHOTO_PATH          as photo_path,
        _FIVETRAN_SYNCED    as _fivetran_synced
    from source
)

select * from renamed
