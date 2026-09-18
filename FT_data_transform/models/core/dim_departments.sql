select
    d.department_id,
    d.department_name,
    r.region_id,
    r.region_name
from {{ ref('stg_departments') }} d
left join {{ ref('stg_regions') }} r
    on safe_cast(d.region_id as int64) = r.region_id
