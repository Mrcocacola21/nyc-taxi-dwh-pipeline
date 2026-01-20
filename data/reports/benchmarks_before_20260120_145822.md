# Benchmarks (before)

Generated: `20260120_145822`

Runs per query: `7` (warmup: `1`)

## Summary (ms)

| phase   | query                     |   count |     min |     max |       mean |   median |
|:--------|:--------------------------|--------:|--------:|--------:|-----------:|---------:|
| before  | q1_top_pickup_zones_day   |       7 |  99.015 | 127.845 | 105.967    |  102.979 |
| before  | q2_mart_daily_revenue     |       7 |   0.198 |   0.503 |   0.320571 |    0.3   |
| before  | q2_revenue_by_day         |       7 | 693.573 | 714.9   | 703.739    |  703.06  |
| before  | q3_join_zone_lookup_top20 |       7 | 393.034 | 520.159 | 434.71     |  411.411 |
| before  | q4_payment_type_stats     |       7 | 228.514 | 277.58  | 242.65     |  236.871 |
| before  | q5_hourly_peak            |       7 | 727.523 | 784.075 | 755.34     |  769.763 |
| before  | q5_mart_hourly_peak       |       7 |   0.291 |   0.556 |   0.402143 |    0.398 |


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

### q2_mart_daily_revenue

```sql
select
          trip_date,
          trips,
          revenue
        from marts.marts_daily_revenue
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

### q5_mart_hourly_peak

```sql
select
          hr,
          sum(trips) as trips
        from marts.marts_hourly_peak
        group by 1
        order by trips desc
```
