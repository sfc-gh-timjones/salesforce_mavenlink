{{
    config(
        materialized='table'
    )
}}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2027-12-31' as date)"
    ) }}
),

final as (
    select
        date_day                                        as date_key,
        date_day                                        as full_date,
        extract(year from date_day)                     as year,
        extract(quarter from date_day)                  as quarter,
        extract(month from date_day)                    as month,
        extract(week from date_day)                     as week_of_year,
        extract(dayofweek from date_day)                as day_of_week,
        extract(dayofyear from date_day)                as day_of_year,
        to_char(date_day, 'YYYY-MM')                    as year_month,
        to_char(date_day, 'YYYY') || '-Q' || extract(quarter from date_day) as year_quarter,
        to_char(date_day, 'Mon')                        as month_name_short,
        to_char(date_day, 'Month')                      as month_name,
        to_char(date_day, 'Dy')                         as day_name_short,
        to_char(date_day, 'Day')                        as day_name,
        case when extract(dayofweek from date_day) in (0, 6) then true else false end as is_weekend,
        date_trunc('month', date_day)                   as first_day_of_month,
        last_day(date_day)                              as last_day_of_month,
        date_trunc('quarter', date_day)                 as first_day_of_quarter,
        date_trunc('year', date_day)                    as first_day_of_year

    from date_spine
)

select * from final
