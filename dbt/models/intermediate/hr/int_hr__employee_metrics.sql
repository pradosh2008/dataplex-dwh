{{ config(materialized='ephemeral') }}

with employees as (
    select * from {{ ref('stg_hr__employees') }}
),
departments as (
    select * from {{ ref('seed_hr__department_lkp') }}
),
enriched as (
    select
        e.employee_id,
        e.site_id,
        e.full_name,
        e.status,
        e.salary,
        e.hire_date,
        e.event_date,
        d.department_name,
        d.cost_centre,
        datediff(e.event_date, e.hire_date)              as tenure_days,
        case when e.status = 'active' then 1 else 0 end  as is_active,
        case
            when e.salary < 60000 then 'junior'
            when e.salary < 80000 then 'mid'
            else 'senior'
        end                                              as salary_band
    from employees as e
    left join departments as d on e.department_id = d.department_id
)
select * from enriched
