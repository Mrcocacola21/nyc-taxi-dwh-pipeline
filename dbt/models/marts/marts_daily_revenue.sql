{{ config(materialized='table') }}

select
  pickup_ts::date as trip_date,
  count(*) as trips,
  sum(total_amount) as revenue
from {{ ref('clean_yellow_trips') }}
group by 1
order by 1
