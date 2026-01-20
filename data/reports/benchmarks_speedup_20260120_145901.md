# Benchmarks (before vs after)

Before: `benchmarks_before_20260120_145822.csv`  
After: `benchmarks_after_20260120_145901.csv`

Median elapsed time per query (ms).

| query | before_ms | after_ms | speedup_x | improvement_pct |
|---|---:|---:|---:|---:|
| q1_top_pickup_zones_day | 103.0 | 35.0 | 2.95 | 66.1% |
| q5_hourly_peak | 769.8 | 635.1 | 1.21 | 17.5% |
| q5_mart_hourly_peak | 0.4 | 0.4 | 1.11 | 9.5% |
| q2_revenue_by_day | 703.1 | 696.7 | 1.01 | 0.9% |
| q3_join_zone_lookup_top20 | 411.4 | 435.8 | 0.94 | -5.9% |
| q4_payment_type_stats | 236.9 | 256.3 | 0.92 | -8.2% |
| q2_mart_daily_revenue | 0.3 | 0.3 | 0.87 | -14.7% |
