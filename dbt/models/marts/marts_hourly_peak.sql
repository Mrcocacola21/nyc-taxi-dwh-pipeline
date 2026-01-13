{{ config(materialized='table') }}

select
  date_trunc('hour', pickup_ts) as pickup_hour,
  extract(hour from pickup_ts)::int as hr,
  count(*) as trips
from clean.clean_yellow_trips
group by 1,2
order by trips desc
