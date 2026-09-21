{{ config(
    location='EU'
) }}

with source as (
    select *
    from {{ source('raw', 'domains') }}
),

renamed as (
    select
        cast(domain_id as int64) as domain_id,
        cast(DomainName as string) as domain_name,
        cast(DomainNameUrl as string) as domain_name_url
    from source
)

select * from renamed
