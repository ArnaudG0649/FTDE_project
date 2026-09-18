select
    region_id,
    region_name,
    count(department_id) as department_count
from {{ ref('dim_departments') }}
group by region_id, region_name
