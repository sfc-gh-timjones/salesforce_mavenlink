with user_bridge as (
    select * from {{ ref('int_cross_system__user_bridge') }}
),

utilization as (
    select
        user_id,
        active_project_count,
        total_hours,
        billable_hours,
        utilization_pct
    from {{ ref('int_mavenlink__user_utilization') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['ub.sf_user_id', 'ub.ml_user_id']) }} as user_key,
        ub.sf_user_id,
        ub.ml_user_id,
        coalesce(ub.sf_full_name, ub.ml_full_name)     as full_name,
        coalesce(ub.sf_email, ub.ml_email)              as email,
        ub.sf_title                                     as title,
        ub.sf_department                                as department,
        ub.sf_is_active                                 as is_active_salesforce,
        ub.ml_headline                                  as mavenlink_headline,
        ub.match_status,

        u.active_project_count                          as ml_active_projects,
        u.total_hours                                   as ml_total_hours,
        u.billable_hours                                as ml_billable_hours,
        u.utilization_pct                               as ml_utilization_pct

    from user_bridge ub
    left join utilization u on ub.ml_user_id = u.user_id
)

select * from final
