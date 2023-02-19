{#
What is the count of records in the model fact_fhv_trips after 
  running all dependencies with the test run variable disabled (:false)

,Create a core model for the stg_fhv_tripdata joining with dim_zones. 
Similar to what we've done in fact_trips 
  keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. 
Run it via the CLI without limits (is_test_run: false) 
  and filter records with pickup time in year 2019.

#}

select 
  fhv.*
  , pu_zone.borough as pickup_borough
  , pu_zone.zone as pickup_zone 
  , do_zone.borough as dropoff_borough 
  , do_zone.zone as dropoff_zone  
from {{ ref('stg_fhv_tripdata') }} fhv
join {{ ref('dim_zones') }} pu_zone on fhv.pickup_locationid = pu_zone.locationid
join {{ ref('dim_zones') }} do_zone on fhv.dropoff_locationid = do_zone.locationid