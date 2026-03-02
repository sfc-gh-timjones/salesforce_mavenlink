with time_entries as (
    select * from {{ ref('stg_mavenlink__time_entry') }}
),

user_info as (
    select * from {{ ref('stg_mavenlink__user') }}
),

time_with_user as (
    select
        te.*,
        u.full_name as user_name,
        u.job_title as user_title
    from time_entries te
    left join user_info u on te.user_id = u.ml_user_id
)

select
    workspace_id,
    
    count(distinct time_entry_id) as total_time_entries,
    count(distinct user_id) as unique_contributors,
    count(distinct date_performed) as days_worked,
    
    round(sum(time_in_hours), 2) as total_hours,
    round(sum(case when is_billable then time_in_hours else 0 end), 2) as billable_hours,
    round(sum(case when not is_billable then time_in_hours else 0 end), 2) as non_billable_hours,
    
    round(sum(revenue_dollars), 2) as total_revenue,
    round(sum(cost_dollars), 2) as total_cost,
    round(sum(revenue_dollars) - sum(cost_dollars), 2) as gross_margin,
    
    round(avg(case when is_billable then rate_dollars end), 2) as avg_bill_rate,
    round(avg(cost_rate_dollars), 2) as avg_cost_rate,
    
    round(
        100.0 * sum(case when is_billable then time_in_hours else 0 end) / 
        nullif(sum(time_in_hours), 0), 
    2) as billability_pct,
    
    min(date_performed) as first_time_entry_date,
    max(date_performed) as last_time_entry_date,
    datediff('day', min(date_performed), max(date_performed)) + 1 as span_days,
    
    max(case 
        when row_num_by_hours = 1 then user_name 
    end) as top_contributor_name,
    max(case 
        when row_num_by_hours = 1 then user_title 
    end) as top_contributor_title,
    max(case 
        when row_num_by_hours = 1 then user_hours 
    end) as top_contributor_hours
    
from (
    select
        t.*,
        row_number() over (
            partition by t.workspace_id 
            order by user_totals.user_hours desc
        ) as row_num_by_hours,
        user_totals.user_hours
    from time_with_user t
    left join (
        select 
            workspace_id, 
            user_id, 
            sum(time_in_hours) as user_hours
        from time_with_user
        group by workspace_id, user_id
    ) user_totals on t.workspace_id = user_totals.workspace_id and t.user_id = user_totals.user_id
)
group by workspace_id
