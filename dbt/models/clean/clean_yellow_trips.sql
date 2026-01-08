{{
  config(
    materialized='incremental',
    unique_key='row_fingerprint',
    incremental_strategy='merge'
  )
}}

with src as (
  select *
  from {{ ref('stg_yellow_trips') }}
),

good as (
  select *
  from src
  where true
    and pickup_ts is not null
    and dropoff_ts is not null
    and dropoff_ts >= pickup_ts
    and pu_location_id is not null
    and do_location_id is not null
    and trip_distance is not null and trip_distance > 0
    and total_amount is not null and total_amount >= 0
    and (payment_type is null or payment_type in (0,1,2,3,4,5,6))
    and (rate_code_id is null or rate_code_id in (1,2,3,4,5,6,99))
),

fingerprinted as (
  select
    *,
    md5(
      coalesce(vendor_id::text,'') || '|' ||
      coalesce(pickup_ts::text,'') || '|' ||
      coalesce(dropoff_ts::text,'') || '|' ||
      coalesce(pu_location_id::text,'') || '|' ||
      coalesce(do_location_id::text,'') || '|' ||
      coalesce(trip_distance::text,'') || '|' ||
      coalesce(total_amount::text,'')
    ) as row_fingerprint
  from good
)

select *
from fingerprinted

{% if is_incremental() %}
  where batch_id not in (select distinct batch_id from {{ this }})
{% endif %}
