with dept_agg as (
    select
        jd.rome_code,
        d.region_id,
        sum(jd.job_offers) as job_offers,
        sum(jd.job_seekers) as job_seekers,
        avg(jd.recruitement_difficulty_score) as recruitement_difficulty_score_avg
    from {{ ref('stg_jobs_departments') }} jd
    left join {{ ref('dim_departments') }} d
        on jd.department_id = d.department_id
    group by jd.rome_code, d.region_id
)

select
    dept_agg.rome_code,
    j.job_name,
    dept_agg.region_id,
    r.region_name,
    dept_agg.job_offers,
    dept_agg.job_seekers,
    safe_divide(dept_agg.job_seekers, dept_agg.job_offers) as job_rate,
    dept_agg.recruitement_difficulty_score_avg
from dept_agg
left join {{ ref('stg_jobs') }} j
    on dept_agg.rome_code = j.rome_code
left join {{ ref('dim_regions') }} r
    on dept_agg.region_id = r.region_id
