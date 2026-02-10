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

## How to regenerate

Run the script from repo root:

```powershell
powershell -ExecutionPolicy Bypass -File docs\explain\run_explains.ps1
```

The script performs:

1. Drop indexes (`sql/perf/000_drop_indexes.sql`) for BEFORE plans.
2. Collect BEFORE plans.
3. Create indexes (`sql/perf/001_create_indexes.sql`).
4. Run `VACUUM (ANALYZE) clean.clean_yellow_trips`.
5. Collect AFTER plans and write all outputs in this directory.

## Notes

- These files are evidence artifacts and are intentionally versioned.
- Regenerate them only on the intended benchmark dataset (for example `2024-01`) to keep comparisons meaningful.
