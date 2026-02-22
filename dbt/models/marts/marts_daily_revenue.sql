{% set mart_window_start_ts_sql = taxi_mart_window_start_ts_sql() %}
{% set mart_window_end_ts_sql = taxi_mart_window_end_ts_sql() %}

{% set pre_hooks = [] %}
{% if is_incremental() %}
  {% do pre_hooks.append("delete from " ~ this ~ " where trip_date >= (" ~ mart_window_start_ts_sql ~ ")::date and trip_date < (" ~ mart_window_end_ts_sql ~ ")::date") %}
{% endif %}

{{
  config(
    materialized='incremental',
    unique_key='trip_date',
    incremental_strategy='delete+insert',
    pre_hook=pre_hooks
  )
}}

with src as (
  select
    pickup_ts,
    total_amount
  from {{ ref('clean_yellow_trips') }}
  {% if is_incremental() %}
    where {{ taxi_mart_incremental_source_filter('pickup_ts') }}
  {% endif %}
)

select
  pickup_ts::date as trip_date,
  count(*) as trips,
  sum(total_amount) as revenue
from src
group by 1
order by 1
