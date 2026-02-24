with products as (
    select * from {{ ref('stg_salesforce__product_2') }}
    where not is_deleted
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
        product_id,
        product_name,
        product_code,
        product_family,
        description,
        is_active,
        currency_code,
        created_at,
        updated_at
    from products
)

select * from final
