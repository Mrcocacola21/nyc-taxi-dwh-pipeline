# Benchmarks (before vs after)

Before: `benchmarks_before_20260110_134736.csv`  
After: `benchmarks_after_20260110_134946.csv`

Median elapsed time per query (ms).

| query | before_ms | after_ms | speedup_x | improvement_pct |
|---|---:|---:|---:|---:|
| q1_top_pickup_zones_day | 119.4 | 36.9 | 3.24 | 69.1% |
| q4_payment_type_stats | 332.4 | 233.9 | 1.42 | 29.6% |
| q3_join_zone_lookup_top20 | 573.1 | 414.3 | 1.38 | 27.7% |
| q5_hourly_peak | 808.5 | 645.9 | 1.25 | 20.1% |
| q2_revenue_by_day | 747.7 | 714.0 | 1.05 | 4.5% |
