select
    region_id,
    region_name
from {{ ref('stg_regions') }}
