{{ config(
    location='EU'
) }}

with source as (
    select *
    from {{ source('raw', 'regions') }}
),

renamed as (
    select
        cast(region_id as int64) as region_id,
        cast(region_name as string) as region_name
    from source
)

select * from renamed
