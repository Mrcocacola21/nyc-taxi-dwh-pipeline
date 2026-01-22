-- sql/perf/000_drop_indexes.sql
SET client_min_messages TO WARNING;

DROP INDEX IF EXISTS clean.idx_clean_pickup_ts;
DROP INDEX IF EXISTS clean.idx_clean_pu_pickup;
DROP INDEX IF EXISTS clean.idx_clean_do;

RESET client_min_messages;
