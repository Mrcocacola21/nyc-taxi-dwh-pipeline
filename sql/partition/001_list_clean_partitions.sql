\set ON_ERROR_STOP on

WITH partitions AS (
  SELECT
    c.oid,
    n.nspname AS schema_name,
    c.relname AS partition_name,
    pg_get_expr(c.relpartbound, c.oid, true) AS bound_expr
  FROM pg_inherits i
  JOIN pg_class p ON p.oid = i.inhparent
  JOIN pg_namespace pn ON pn.oid = p.relnamespace
  JOIN pg_class c ON c.oid = i.inhrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE pn.nspname = 'clean'
    AND p.relname = 'clean_yellow_trips'
)
SELECT
  format('%I.%I', schema_name, partition_name) AS partition_name,
  bound_expr AS partition_bound,
  coalesce(s.n_live_tup::bigint, 0) AS estimated_rows,
  pg_size_pretty(pg_total_relation_size(oid)) AS total_size
FROM partitions p
LEFT JOIN pg_stat_user_tables s ON s.relid = p.oid
ORDER BY partition_name;
