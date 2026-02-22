{% set mart_window_start_ts_sql = taxi_mart_window_start_ts_sql() %}
{% set mart_window_end_ts_sql = taxi_mart_window_end_ts_sql() %}

{% set pre_hooks = [] %}
{% if is_incremental() %}
  {% do pre_hooks.append("delete from " ~ this ~ " where trip_date >= (" ~ mart_window_start_ts_sql ~ ")::date and trip_date < (" ~ mart_window_end_ts_sql ~ ")::date") %}
{% endif %}

{{
  config(
    materialized='incremental',
    unique_key=['trip_date', 'pu_location_id'],
    incremental_strategy='delete+insert',
    pre_hook=pre_hooks
  )
}}

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
  {% if is_incremental() %}
    where {{ taxi_mart_incremental_source_filter('pickup_ts') }}
  {% endif %}
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
