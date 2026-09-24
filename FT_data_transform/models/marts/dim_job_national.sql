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
    dept_agg.job_seekers as job_seekers_aggregated,
    dept_agg.job_offers as job_offers_aggregated,
    j.job_seekers_national,
    j.job_offers_national,
    safe_divide(j.job_offers_national, j.job_seekers_national) as offers_per_seeker_national,
    j.salaryq10,
    j.salaryq90,
    j.recruitement_difficulty_score,
    j.recruitement_difficulty_score_year,
    j.source_period
from {{ ref('stg_jobs') }} j
left join dept_agg
    on j.rome_code = dept_agg.rome_code
