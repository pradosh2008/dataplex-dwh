{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['event_date', 'site_id', 'department_name', 'cost_centre', 'salary_band'],
        partition_by=['event_date', 'site_id'],
        cluster_by=['department_name'],
        tags=['hr']
    )
}}

with employee_metrics as (
    select * from {{ ref('int_hr__employee_metrics') }}
    {% if var("start_of_backfill_window", false) and var("end_of_backfill_window", false) %}
        where event_date between '{{ var("start_of_backfill_window") }}' and '{{ var("end_of_backfill_window") }}'
    {% else %}
        {% if is_incremental() %}
            where event_date > date_sub((select max(event_date) from {{ this }}), 2)
        {% endif %}
    {% endif %}
),
aggregated as (
    select
        event_date,
        site_id,
        department_name,
        cost_centre,
        salary_band,
        count(employee_id)      as employee_count,
        sum(is_active)          as active_employee_count,
        avg(salary)             as avg_salary,
        avg(tenure_days)        as avg_tenure_days
    from employee_metrics
    group by 1, 2, 3, 4, 5
)
select * from aggregated
