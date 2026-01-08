with src as (
  select * from raw.yellow_trips
),

typed as (
  select
    batch_id,
    vendorid::bigint as vendor_id,
    tpep_pickup_datetime as pickup_ts,
    tpep_dropoff_datetime as dropoff_ts,
    passenger_count::numeric as passenger_count,
    trip_distance::numeric as trip_distance,
    ratecodeid::bigint as rate_code_id,
    store_and_fwd_flag,
    pulocationid::bigint as pu_location_id,
    dolocationid::bigint as do_location_id,
    payment_type::bigint as payment_type,
    fare_amount::numeric as fare_amount,
    tip_amount::numeric as tip_amount,
    total_amount::numeric as total_amount,
    congestion_surcharge::numeric as congestion_surcharge,
    airport_fee::numeric as airport_fee,
    cbd_congestion_fee::numeric as cbd_congestion_fee,
    ingested_at
  from src
),

enriched as (
  select
    *,
    extract(epoch from (dropoff_ts - pickup_ts)) as trip_duration_sec,
    case
      when extract(epoch from (dropoff_ts - pickup_ts)) > 0
      then (trip_distance / (extract(epoch from (dropoff_ts - pickup_ts))/3600.0))
      else null
    end as avg_speed_kmh
  from typed
)

select * from enriched
