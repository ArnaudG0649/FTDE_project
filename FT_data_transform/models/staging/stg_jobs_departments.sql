{{ config(
    location='EU'
) }}

with source as (
    select *
    from {{ source('raw', 'jobs_departments') }}
),

renamed as (
    select
        cast(romeCode as string) as rome_code,
        cast(department_id as string) as department_id,
        cast(jobSeekers as int64) as job_seekers,
        cast(jobOffers as int64) as job_offers,
        cast(sourcePeriod as string) as source_period,
        cast(salaryq10 as int64) as salaryq10,
        cast(salaryq90 as int64) as salaryq90,
        cast(recruitementDifficultyScore as int64) as recruitement_difficulty_score,
        cast(recruitementDifficultyScoreYear as int64) as recruitement_difficulty_score_year
    from source
)

select * from renamed
