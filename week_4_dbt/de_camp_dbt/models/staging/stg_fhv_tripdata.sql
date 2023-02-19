{{ 
  config(
    materialized='view'
  ) 
}}

select
  dispatching_base_num
  , pickup_datetime
  , dropOff_datetime as dropoff_datetime
  , PUlocationID as pickup_locationid
  , DOlocationID as dropoff_locationid
  , SR_Flag as sr_flag
  , Affiliated_base_number as affiliated_base_number

from {{ source('src_trips','physical_fhv') }}
