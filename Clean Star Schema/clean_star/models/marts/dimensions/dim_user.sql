with sf_users_raw as (
    select
        *,
        row_number() over (partition by lower(email) order by is_active desc, sf_user_id) as rn
    from {{ ref('stg_salesforce__user') }}
    where email is not null
),

sf_users as (
    select * from sf_users_raw where rn = 1
),

sf_users_no_email as (
    select *, 1 as rn from {{ ref('stg_salesforce__user') }} where email is null
),

ml_users_raw as (
    select
        *,
        row_number() over (partition by lower(email) order by ml_user_id) as rn
    from {{ ref('stg_mavenlink__user') }}
    where email is not null
),

ml_users as (
    select * from ml_users_raw where rn = 1
),

time_summary as (
    select
        user_id,
        sum(time_in_hours) as total_hours,
        sum(case when is_billable then time_in_hours else 0 end) as billable_hours,
        count(distinct workspace_id) as active_projects,
        round(
            100.0 * sum(case when is_billable then time_in_hours else 0 end) / 
            nullif(sum(time_in_hours), 0),
        2) as utilization_pct
    from {{ ref('stg_mavenlink__time_entry') }}
    group by user_id
),

matched_users as (
    select
        sf.sf_user_id,
        ml.ml_user_id,
        coalesce(sf.full_name, ml.full_name) as full_name,
        coalesce(sf.email, ml.email) as email,
        sf.title as sf_title,
        ml.job_title as ml_title,
        sf.department,
        sf.is_active as is_active_salesforce,
        sf.manager_id,
        case
            when sf.sf_user_id is not null and ml.ml_user_id is not null then 'Matched'
            when sf.sf_user_id is not null then 'Salesforce Only'
            else 'Mavenlink Only'
        end as match_status
    from sf_users sf
    full outer join ml_users ml 
        on lower(sf.email) = lower(ml.email)

    union all

    select
        sf.sf_user_id,
        null as ml_user_id,
        sf.full_name,
        sf.email,
        sf.title as sf_title,
        null as ml_title,
        sf.department,
        sf.is_active as is_active_salesforce,
        sf.manager_id,
        'Salesforce Only' as match_status
    from sf_users_no_email sf
)

select
    sf_user_id,
    ml_user_id,
    full_name,
    email,
    sf_title,
    ml_title,
    department,
    is_active_salesforce,
    manager_id,
    match_status,
    
    coalesce(ts.active_projects, 0) as ml_active_projects,
    coalesce(ts.total_hours, 0) as ml_total_hours,
    coalesce(ts.billable_hours, 0) as ml_billable_hours,
    ts.utilization_pct as ml_utilization_pct
    
from matched_users mu
left join time_summary ts on mu.ml_user_id = ts.user_id
