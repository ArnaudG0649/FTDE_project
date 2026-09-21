{{ config(
    location='EU'
) }}

with source as (
    select *
    from {{ source('raw', 'jobs_subdomains') }}
),

renamed as (
    select
        cast(romeCode as string) as rome_code,
        cast(subdomain_id as int64) as subdomain_id
    from source
)

select * from renamed
