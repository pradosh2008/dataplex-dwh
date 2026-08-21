{{ config(materialized='view') }}

with source as (
    select * from {{ source('hr_raw', 'raw_hr__employees') }}
),
renamed as (
    select
        employee_id,
        site_id,
        department_id,
        full_name,
        status,
        salary,
        hire_date,
        event_date,
        current_timestamp() as _loaded_at
    from source
)
select * from renamed
