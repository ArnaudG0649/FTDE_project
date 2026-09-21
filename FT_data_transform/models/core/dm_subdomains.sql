select
    b.subdomain_id,
    b.subdomain_name,
    sum(n.job_offers) as job_offers,
    sum(n.job_seekers) as job_seekers,
    avg(n.recruitement_difficulty_score) as recruitement_difficulty_score_avg
from {{ ref('bridge_jobs_subdomains') }} b
left join {{ ref('dim_job_national') }} n
    on b.rome_code = n.rome_code
group by b.subdomain_id, b.subdomain_name
