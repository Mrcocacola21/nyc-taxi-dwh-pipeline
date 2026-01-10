# NYC Taxi DWH Pipeline (Postgres + dbt + Great Expectations)

Goal: build an incremental ELT pipeline with data quality quarantine and benchmarks.

## Quick start
1) Copy env
```bash
cp .env.example .env

## Query Plan Analysis (EXPLAIN ANALYZE)

We captured Postgres execution plans before and after creating indexes on `clean.clean_yellow_trips`.

### q1: Top pickup zones for a single day (filter by pickup_ts)
- Before: **Parallel Seq Scan** on `clean_yellow_trips`, ~924k rows removed by filter.
  - Buffers: `read=67363` (~526 MB), `hit=9965`
  - Execution Time: **1120.8 ms**
- After: **Index Scan** using `idx_clean_pickup_ts` (range condition on pickup_ts).
  - Buffers: `read=219` (~1.7 MB), `hit=98640`
  - Execution Time: **73.3 ms**

**Conclusion:** index changed the access path from full table scan to range scan, drastically reducing I/O and latency.

### q3: Join with taxi_zone_lookup + top zones (full-table aggregation)
- Before/After: plan remains **Hash Join + Parallel Seq Scan** on trips.
- Indexes do not change the plan because the query aggregates over (almost) the entire dataset.

**Conclusion:** for global aggregations, performance improvements should come from data modeling (pre-aggregated marts/materialized tables) rather than indexes alone.
