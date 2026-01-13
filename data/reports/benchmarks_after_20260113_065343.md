# Benchmarks (after)

Generated: `20260113_065343`

Runs per query: `7` (warmup: `1`)

## Summary (ms)

| phase   | query                     |   count |     min |     max |     mean |   median |
|:--------|:--------------------------|--------:|--------:|--------:|---------:|---------:|
| after   | q1_top_pickup_zones_day   |       7 |  36.051 |  36.673 |  36.3287 |   36.315 |
| after   | q2_revenue_by_day         |       7 | 757.132 | 794.297 | 765.482  |  761.229 |
| after   | q3_join_zone_lookup_top20 |       7 | 404.505 | 477.423 | 437.052  |  435.092 |
| after   | q4_payment_type_stats     |       7 | 251.401 | 329.783 | 275.829  |  274.946 |
| after   | q5_hourly_peak            |       7 | 682.663 | 725.543 | 698.045  |  694.792 |


## Queries

### q1_top_pickup_zones_day

```sql
select
          pu_location_id,
          count(*) as trips
        from clean.clean_yellow_trips
        where pickup_ts >= timestamp '2024-01-31 00:00:00'
          and pickup_ts <  timestamp '2024-02-01 00:00:00'
        group by 1
        order by trips desc
        limit 20
```

### q2_revenue_by_day

```sql
select
          pickup_ts::date as trip_date,
          count(*) as trips,
          sum(total_amount) as revenue
        from clean.clean_yellow_trips
        group by 1
        order by 1
```

### q3_join_zone_lookup_top20

```sql
select
          z.borough,
          z.zone,
          count(*) as trips,
          avg(t.total_amount) as avg_total
        from clean.clean_yellow_trips t
        join raw.taxi_zone_lookup z
          on z.locationid = t.pu_location_id
        group by 1, 2
        order by trips desc
        limit 20
```

### q4_payment_type_stats

```sql
select
          payment_type,
          count(*) as trips,
          avg(tip_amount) as avg_tip
        from clean.clean_yellow_trips
        group by 1
        order by trips desc
```

### q5_hourly_peak

```sql
select
          extract(hour from pickup_ts)::int as hr,
          count(*) as trips
        from clean.clean_yellow_trips
        group by 1
        order by trips desc
```
