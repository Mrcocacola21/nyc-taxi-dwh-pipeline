# Benchmarks (before)

Generated: `20260110_134736`

Runs per query: `7` (warmup: `1`)

## Summary (ms)

| phase   | query                     |   count |     min |     max |    mean |   median |
|:--------|:--------------------------|--------:|--------:|--------:|--------:|---------:|
| before  | q1_top_pickup_zones_day   |       7 | 118.868 | 121.056 | 119.675 |  119.422 |
| before  | q2_revenue_by_day         |       7 | 743.06  | 774.452 | 750.889 |  747.659 |
| before  | q3_join_zone_lookup_top20 |       7 | 534.509 | 678.088 | 594.365 |  573.105 |
| before  | q4_payment_type_stats     |       7 | 317.091 | 367.209 | 338.118 |  332.353 |
| before  | q5_hourly_peak            |       7 | 797.75  | 845.733 | 812.223 |  808.509 |


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
