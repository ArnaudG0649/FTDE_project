select
    domain_id,
    domain_name,
    domain_name_url
from {{ ref('stg_domains') }}
