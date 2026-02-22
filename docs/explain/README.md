# EXPLAIN Evidence

This folder stores reproducible `EXPLAIN (ANALYZE, BUFFERS)` outputs used in the thesis/performance narrative.

## Canonical plan files

- `q1_top_pickup_zones_day_before.txt`
- `q1_top_pickup_zones_day_after.txt`
- `q2_revenue_by_day_before.txt`
- `q2_revenue_by_day_after.txt`
- `q3_join_zone_lookup_top20_before.txt`
- `q3_join_zone_lookup_top20_after.txt`
- `q4_payment_type_stats_before.txt`
- `q4_payment_type_stats_after.txt`
- `q5_hourly_peak_before.txt`
- `q5_hourly_peak_after.txt`
- `q2_clean.txt`
- `q2_mart.txt`
- `q5_clean.txt`
- `q5_mart.txt`

## Partitioning evidence files

- `q1_partition_pruning_before_heap.txt`
- `q1_partition_pruning_after_partitioned.txt`
- `q2_partition_scope_before_heap.txt`
- `q2_partition_scope_after_partitioned.txt`
- `q1_partition_hits_after_partitioned.txt`
- `clean_yellow_trips_partition_tree.txt`
- `clean_yellow_trips_partitions.txt`

## How to regenerate

Run the script from repo root:

```powershell
powershell -ExecutionPolicy Bypass -File docs\explain\run_explains.ps1
```

Partition-focused evidence (pruning and scanned scope):

```powershell
powershell -ExecutionPolicy Bypass -File docs\explain\run_partition_explains.ps1
```

The script performs:

1. Drop indexes (`sql/perf/000_drop_indexes.sql`) for BEFORE plans.
2. Collect BEFORE plans.
3. Create indexes (`sql/perf/001_create_indexes.sql`).
4. Run `VACUUM (ANALYZE) clean.clean_yellow_trips`.
5. Collect AFTER plans and write all outputs in this directory.

`run_partition_explains.ps1` performs:

1. Ensures `clean.clean_yellow_trips` is partitioned (migrates if needed).
2. Drops perf indexes to isolate partition-pruning impact.
3. Creates an unpartitioned heap baseline snapshot (`clean.clean_yellow_trips_heap_baseline`).
4. Captures before/after plans (heap baseline vs partitioned table) with `BUFFERS`.
5. Captures partition inventory and partition-hit evidence.

## Notes

- These files are evidence artifacts and are intentionally versioned.
- Regenerate them only on the intended benchmark dataset (for example `2024-01`) to keep comparisons meaningful.
