with source as (
    select * from {{ source('salesforce', 'OPPORTUNITY') }}
    where not coalesce(_FIVETRAN_DELETED, false)
),

renamed as (
    select
        ID                          as opportunity_id,
        ACCOUNT_ID                  as account_id,
        OWNER_ID                    as owner_id,
        NAME                        as opportunity_name,
        DESCRIPTION                 as description,
        STAGE_NAME                  as stage_name,
        AMOUNT::number(38,2)        as amount,
        PROBABILITY                 as probability,
        CLOSE_DATE                  as close_date,
        TYPE                        as opportunity_type,
        LEAD_SOURCE                 as lead_source,
        NEXT_STEP                   as next_step,
        FORECAST_CATEGORY           as forecast_category,
        FORECAST_CATEGORY_NAME      as forecast_category_name,
        IS_CLOSED                   as is_closed,
        IS_WON                      as is_won,
        HAS_OPPORTUNITY_LINE_ITEM   as has_line_items,
        PRICEBOOK_2_ID              as pricebook_id,
        CAMPAIGN_ID                 as campaign_id,
        RECORD_TYPE_ID              as record_type_id,
        CURRENCY_ISO_CODE           as currency_code,
        IS_DELETED                  as is_deleted,
        CREATED_DATE                as created_at,
        LAST_MODIFIED_DATE          as updated_at,
        LAST_ACTIVITY_DATE          as last_activity_date,
        PUSH_COUNT                  as push_count,
        FISCAL_QUARTER              as fiscal_quarter,
        FISCAL_YEAR                 as fiscal_year,
        _FIVETRAN_SYNCED            as _fivetran_synced
    from source
)

select * from renamed
