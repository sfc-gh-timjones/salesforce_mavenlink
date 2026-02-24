with source as (
    select * from {{ source('salesforce', 'USER') }}
),

renamed as (
    select
        ID                          as user_id,
        USERNAME                    as username,
        FIRST_NAME                  as first_name,
        LAST_NAME                   as last_name,
        NAME                        as full_name,
        EMAIL                       as email,
        TITLE                       as title,
        DEPARTMENT                  as department,
        DIVISION                    as division,
        COMPANY_NAME                as company_name,
        PHONE                       as phone,
        CITY                        as city,
        STATE                       as state,
        COUNTRY                     as country,
        IS_ACTIVE                   as is_active,
        CREATED_DATE                as created_at,
        LAST_MODIFIED_DATE          as updated_at,
        LAST_LOGIN_DATE             as last_login_at,
        _FIVETRAN_SYNCED            as _fivetran_synced
    from source
)

select * from renamed
