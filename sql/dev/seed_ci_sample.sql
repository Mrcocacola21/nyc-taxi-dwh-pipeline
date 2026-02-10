-- Small deterministic sample for quick local and CI-like validation.
-- Safe to run repeatedly.

TRUNCATE raw.yellow_trips;
TRUNCATE raw.taxi_zone_lookup;

INSERT INTO raw.taxi_zone_lookup (locationid, borough, zone, service_zone) VALUES
  (1, 'Manhattan', 'Financial District', 'Yellow Zone'),
  (2, 'Queens', 'JFK Airport', 'Airports'),
  (3, 'Brooklyn', 'Williamsburg', 'Boro Zone'),
  (4, 'Manhattan', 'Midtown Center', 'Yellow Zone');

INSERT INTO raw.yellow_trips (
  batch_id,
  vendorid,
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  passenger_count,
  trip_distance,
  ratecodeid,
  store_and_fwd_flag,
  pulocationid,
  dolocationid,
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  airport_fee,
  cbd_congestion_fee,
  ingested_at
)
VALUES
  ('2024-01', 1, '2024-01-31 00:10:00', '2024-01-31 00:25:00', 1, 2.1, 1, 'N', 1, 2, 1, 12.00, 0.5, 0.5, 2.5, 0.0, 0.3, 15.80, 2.5, 0.0, 0.0, now()),
  ('2024-01', 2, '2024-01-31 07:15:00', '2024-01-31 07:35:00', 2, 4.3, 1, 'N', 2, 1, 2, 18.00, 0.5, 0.5, 3.0, 0.0, 0.3, 22.30, 2.5, 0.0, 0.0, now()),
  ('2024-01', 1, '2024-01-31 12:05:00', '2024-01-31 12:20:00', 1, 1.8, 1, 'N', 1, 3, 1, 10.00, 0.5, 0.5, 1.5, 0.0, 0.3, 12.80, 2.5, 0.0, 0.0, now()),
  ('2024-01', 2, '2024-01-31 18:00:00', '2024-01-31 18:28:00', 3, 6.2, 1, 'N', 3, 4, 3, 24.00, 1.0, 0.5, 4.0, 0.0, 0.3, 29.80, 2.5, 0.0, 0.0, now()),
  ('2024-01', 1, '2024-02-01 09:00:00', '2024-02-01 09:18:00', 1, 3.6, 1, 'N', 4, 1, 1, 15.00, 0.5, 0.5, 2.0, 0.0, 0.3, 18.30, 2.5, 0.0, 0.0, now());
