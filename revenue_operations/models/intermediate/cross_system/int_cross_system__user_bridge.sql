{{
    config(
        materialized='table'
    )
}}

with sf_users as (
    select
        user_id         as sf_user_id,
        full_name       as sf_full_name,
        email           as sf_email,
        title           as sf_title,
        department      as sf_department,
        is_active       as sf_is_active,
        row_number() over (partition by lower(email) order by user_id) as rn
    from {{ ref('stg_salesforce__user') }}
),

sf_deduped as (
    select * from sf_users where rn = 1
),

ml_users as (
    select
        user_id         as ml_user_id,
        full_name       as ml_full_name,
        email_address   as ml_email,
        headline        as ml_headline,
        row_number() over (partition by lower(email_address) order by user_id) as rn
    from {{ ref('stg_mavenlink__user') }}
),

ml_deduped as (
    select * from ml_users where rn = 1
),

bridge as (
    select
        sf.sf_user_id,
        sf.sf_full_name,
        sf.sf_email,
        sf.sf_title,
        sf.sf_department,
        sf.sf_is_active,

        ml.ml_user_id,
        ml.ml_full_name,
        ml.ml_email,
        ml.ml_headline,

        case
            when sf.sf_user_id is not null and ml.ml_user_id is not null then 'Matched'
            when sf.sf_user_id is not null then 'Salesforce Only'
            else 'Mavenlink Only'
        end as match_status

    from sf_deduped sf
    full outer join ml_deduped ml
        on lower(sf.sf_email) = lower(ml.ml_email)
)

select * from bridge
