# EXPLAIN Evidence

## q1_top_pickup_zones_day — Impact of indexing

**Predicate:** pickup_ts in [2024-01-31, 2024-02-01)

### Before (no index)
- Plan: `Finalize GroupAggregate` + `Gather Merge` over workers (parallel)
- Buffers: `shared hit=9870 read=67458`
- Runtime: `actual time ~869–873 ms` (top node)

See: `q1_before.txt`

### After (idx_clean_pickup_ts)
- Plan: `Index Scan using idx_clean_pickup_ts on clean_yellow_trips`
- Buffers: `shared hit=98630 read=219`
- Runtime: `Execution Time: 58.277 ms`

See: `q1_after.txt`

**Conclusion:** The access path changes from a near-full scan to an index range scan, cutting disk reads from **67458** to **219** pages (≈ **99.7%** less reads) and reducing latency by ~**15×** for this selective query.
