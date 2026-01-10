with trips as (
  select
    pickup_ts::date as trip_date,
    pu_location_id,
    do_location_id,
    total_amount,
    tip_amount,
    trip_distance,
    trip_duration_sec
  from {{ ref('clean_yellow_trips') }}
),

agg as (
  select
    trip_date,
    pu_location_id,
    count(*) as trips,
    avg(total_amount) as avg_total_amount,
    avg(tip_amount) as avg_tip_amount,
    avg(trip_distance) as avg_distance,
    avg(trip_duration_sec) as avg_duration_sec
  from trips
  group by 1,2
)

select * from agg
