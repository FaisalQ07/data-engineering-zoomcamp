{{
    config(
        materialized='table'
    )
}}

with green_tripdata as (
    select *, 
        'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
), 
 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select green_tripdata.tripid, 
    green_tripdata.vendorid, 
    green_tripdata.service_type,
    green_tripdata.ratecodeid, 
    green_tripdata.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    green_tripdata.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    green_tripdata.pickup_datetime, 
    green_tripdata.dropoff_datetime, 
    green_tripdata.store_and_fwd_flag, 
    green_tripdata.passenger_count, 
    green_tripdata.trip_distance, 
    green_tripdata.trip_type, 
    green_tripdata.fare_amount, 
    green_tripdata.extra, 
    green_tripdata.mta_tax, 
    green_tripdata.tip_amount, 
    green_tripdata.tolls_amount, 
    green_tripdata.ehail_fee, 
    green_tripdata.improvement_surcharge, 
    green_tripdata.total_amount, 
    green_tripdata.payment_type, 
    green_tripdata.payment_type_description
from green_tripdata
inner join dim_zones as pickup_zone
on green_tripdata.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on green_tripdata.dropoff_locationid = dropoff_zone.locationid