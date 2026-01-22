-- sql/perf/001_create_indexes.sql
SET client_min_messages TO WARNING;

-- q1: фильтр по pickup_ts + группировка по pu_location_id
CREATE INDEX IF NOT EXISTS idx_clean_pickup_ts
  ON clean.clean_yellow_trips (pickup_ts)
  INCLUDE (pu_location_id);

CREATE INDEX IF NOT EXISTS idx_clean_pu_pickup
  ON clean.clean_yellow_trips (pu_location_id, pickup_ts);

CREATE INDEX IF NOT EXISTS idx_clean_do
  ON clean.clean_yellow_trips (do_location_id);

ANALYZE clean.clean_yellow_trips;

RESET client_min_messages;
