with source as (
    select * from {{ source('salesforce', 'OPPORTUNITY_LINE_ITEM') }}
    where not coalesce(_FIVETRAN_DELETED, false)
),

renamed as (
    select
        ID                          as opportunity_line_item_id,
        OPPORTUNITY_ID              as opportunity_id,
        PRODUCT_2_ID                as product_id,
        PRICEBOOK_ENTRY_ID          as pricebook_entry_id,
        NAME                        as line_item_name,
        PRODUCT_CODE                as product_code,
        QUANTITY                    as quantity,
        UNIT_PRICE::number(38,2)    as unit_price,
        TOTAL_PRICE::number(38,2)  as total_price,
        LIST_PRICE::number(38,2)   as list_price,
        DISCOUNT                    as discount_percent,
        SERVICE_DATE                as service_date,
        DESCRIPTION                 as description,
        SORT_ORDER                  as sort_order,
        CURRENCY_ISO_CODE           as currency_code,
        IS_DELETED                  as is_deleted,
        CREATED_DATE                as created_at,
        LAST_MODIFIED_DATE          as updated_at,
        _FIVETRAN_SYNCED            as _fivetran_synced
    from source
)

select * from renamed
