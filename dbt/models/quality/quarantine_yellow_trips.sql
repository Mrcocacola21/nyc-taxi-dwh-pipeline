with t as (
  select * from {{ ref('stg_yellow_trips') }}
),

bad as (
  select
    *,
    case
      when pickup_ts is null or dropoff_ts is null then 'missing_datetime'
      when dropoff_ts < pickup_ts then 'negative_duration'
      when pu_location_id is null or do_location_id is null then 'missing_location'
      when trip_distance is null or trip_distance <= 0 then 'invalid_distance'
      when total_amount is null or total_amount < 0 then 'invalid_total_amount'
      when payment_type is not null and payment_type not in (0,1,2,3,4,5,6) then 'invalid_payment_type'
      when rate_code_id is not null and rate_code_id not in (1,2,3,4,5,6,99) then 'invalid_rate_code'
      when avg_speed_kmh is not null and avg_speed_kmh > 200 then 'unrealistic_speed'
      else null
    end as reason_code
  from t
)

select *
from bad
where reason_code is not null
