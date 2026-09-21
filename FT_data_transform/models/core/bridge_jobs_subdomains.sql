select
    js.rome_code,
    j.job_name,
    js.subdomain_id,
    sd.subdomain_name,
    sd.domain_id
from {{ ref('stg_jobs_subdomains') }} js
left join {{ ref('stg_jobs') }} j
    on js.rome_code = j.rome_code
left join {{ ref('stg_subdomains') }} sd
    on js.subdomain_id = sd.subdomain_id
