\set ON_ERROR_STOP on

-- Installs helper functions and migrates clean.clean_yellow_trips to a
-- monthly range-partitioned table if it is still heap-based.
\i /app/sql/init/003_clean_partitioning.sql

SELECT clean.ensure_clean_yellow_trips_partitioned();

ANALYZE clean.clean_yellow_trips;
