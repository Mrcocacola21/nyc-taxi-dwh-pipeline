# Benchmarks (before vs after)

Before: `benchmarks_before_20260120_145415.csv`  
After: `benchmarks_after_20260120_145535.csv`

Median elapsed time per query (ms).

| query | before_ms | after_ms | speedup_x | improvement_pct |
|---|---:|---:|---:|---:|
| q1_top_pickup_zones_day | 95.7 | 35.4 | 2.70 | 63.0% |
| q5_hourly_peak | 736.6 | 637.7 | 1.16 | 13.4% |
| q5_mart_hourly_peak | 0.4 | 0.3 | 1.05 | 4.4% |
| q3_join_zone_lookup_top20 | 402.7 | 400.3 | 1.01 | 0.6% |
| q2_revenue_by_day | 699.0 | 720.5 | 0.97 | -3.1% |
| q2_mart_daily_revenue | 0.3 | 0.3 | 0.95 | -5.0% |
| q4_payment_type_stats | 244.2 | 261.0 | 0.94 | -6.9% |
