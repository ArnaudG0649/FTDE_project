select
    d.department_id,
    d.department_name,
    j.job_name,
    jd.job_seekers,
    jd.job_offers,
    safe_divide(jd.job_offers, jd.job_seekers) as offers_per_seeker,
    jd.job_period,
    jd.salaryq10,
    jd.salaryq90,
    jd.recruitement_difficulty_score,
    jd.recruitement_difficulty_score_year
from {{ ref('stg_jobs_departments') }} jd
left join {{ ref('dim_departments') }} d
    on jd.department_id = d.department_id
left join {{ ref('stg_jobs') }} j
    on jd.rome_code = j.rome_code
