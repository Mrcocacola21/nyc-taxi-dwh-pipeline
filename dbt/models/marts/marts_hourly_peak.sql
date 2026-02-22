{% set mart_window_start_ts_sql = taxi_mart_window_start_ts_sql() %}
{% set mart_window_end_ts_sql = taxi_mart_window_end_ts_sql() %}

{% set pre_hooks = [] %}
{% if is_incremental() %}
  {% do pre_hooks.append("delete from " ~ this ~ " where pickup_hour >= date_trunc('hour', " ~ mart_window_start_ts_sql ~ ") and pickup_hour < date_trunc('hour', " ~ mart_window_end_ts_sql ~ ")") %}
{% endif %}

{{
  config(
    materialized='incremental',
    unique_key='pickup_hour',
    incremental_strategy='delete+insert',
    pre_hook=pre_hooks
  )
}}

with src as (
  select
    pickup_ts
  from {{ ref('clean_yellow_trips') }}
  {% if is_incremental() %}
    where {{ taxi_mart_incremental_source_filter('pickup_ts') }}
  {% endif %}
)

select
  date_trunc('hour', pickup_ts) as pickup_hour,
  extract(hour from pickup_ts)::int as hr,
  count(*) as trips
from src
group by 1,2
order by trips desc
