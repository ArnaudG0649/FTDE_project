with dept_agg as (
    select
        rome_code,
        sum(job_offers) as job_offers,
        sum(job_seekers) as job_seekers
    from {{ ref('stg_jobs_departments') }}
    group by rome_code
)

select
    j.rome_code,
    j.job_name,
    dept_agg.job_offers,
    dept_agg.job_seekers,
    safe_divide(dept_agg.job_offers, dept_agg.job_seekers) as offers_per_seeker,
    j.salaryq10,
    j.salaryq90,
    j.recruitement_difficulty_score
from {{ ref('stg_jobs') }} j
left join dept_agg
    on j.rome_code = dept_agg.rome_code
