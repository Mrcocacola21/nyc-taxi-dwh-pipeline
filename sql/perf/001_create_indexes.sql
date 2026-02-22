-- sql/perf/001_create_indexes.sql
SET client_min_messages TO WARNING;

-- Parent-level index DDL.
-- On partitioned clean.clean_yellow_trips this creates partitioned indexes and
-- matching indexes on each existing partition.

-- q1: time-window filter on pickup_ts + group by pu_location_id.
CREATE INDEX IF NOT EXISTS idx_clean_pickup_ts
  ON clean.clean_yellow_trips (pickup_ts)
  INCLUDE (pu_location_id);

CREATE INDEX IF NOT EXISTS idx_clean_pu_pickup
  ON clean.clean_yellow_trips (pu_location_id, pickup_ts);

CREATE INDEX IF NOT EXISTS idx_clean_do
  ON clean.clean_yellow_trips (do_location_id);

ANALYZE clean.clean_yellow_trips;

RESET client_min_messages;
