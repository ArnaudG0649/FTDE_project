{{ config(
    location='EU'
) }}

with source as (
    select *
    from {{ source('raw', 'departments') }}
),

renamed as (
    select
        cast(department_id as string) as department_id,
        cast(department_name as string) as department_name,
        cast(region_id as string) as region_id
    from source
)

select * from renamed
