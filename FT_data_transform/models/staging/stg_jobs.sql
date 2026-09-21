{{ config(
    location='EU'
) }}

with source as (
    select *
    from {{ source('raw', 'jobs') }}
),

renamed as (
    select
        cast(romeCode as string) as rome_code,
        cast(job_name as string) as job_name,
        cast(transitionNumerique as bool) as transition_numerique,
        cast(transitionDemographique as bool) as transition_demographique,
        cast(transitionEcologique as bool) as transition_ecologique,
        cast(emploiCadre as bool) as emploi_cadre,
        cast(emploiReglemente as bool) as emploi_reglemente,
        cast(salaryq10 as int64) as salaryq10,
        cast(salaryq90 as int64) as salaryq90,
        cast(recruitementDifficultyScore as int64) as recruitement_difficulty_score,
        cast(recruitementDifficultyScoreYear as int64) as recruitement_difficulty_score_year
    from source
)

select * from renamed
