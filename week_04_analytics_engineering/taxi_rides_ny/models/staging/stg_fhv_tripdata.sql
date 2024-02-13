{{ config( materialized='view')}}

select *
from {{ source('staging', 'fhv_tripdata_non_partitioned')}}

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 1000

{% endif %}