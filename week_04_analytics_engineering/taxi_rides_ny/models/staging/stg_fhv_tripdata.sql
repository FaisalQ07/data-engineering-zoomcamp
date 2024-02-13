{{ config( materialized='view')}}

select *,
EXTRACT(YEAR FROM pickup_datetime) AS pick_up_year
from {{ source('staging', 'fhv_tripdata_non_partitioned')}}
where EXTRACT(YEAR FROM pickup_datetime) = 2019

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 1000

{% endif %}