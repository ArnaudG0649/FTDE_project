{{ config(
    location='EU'
) }}

with source as (
    select *
    from {{ source('raw', 'subdomains') }}
),

renamed as (
    select
        cast(subdomain_id as int64) as subdomain_id,
        cast(subDomain as string) as subdomain_name,
        cast(domain_id as int64) as domain_id
    from source
)

select * from renamed
