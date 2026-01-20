# Benchmarks (before)

Generated: `20260120_145415`

Runs per query: `7` (warmup: `1`)

## Summary (ms)

| phase   | query                     |   count |     min |     max |       mean |   median |
|:--------|:--------------------------|--------:|--------:|--------:|-----------:|---------:|
| before  | q1_top_pickup_zones_day   |       7 |  95.065 | 111.391 |  98.2016   |   95.707 |
| before  | q2_mart_daily_revenue     |       7 |   0.202 |   0.634 |   0.323143 |    0.279 |
| before  | q2_revenue_by_day         |       7 | 695.305 | 717.477 | 702.236    |  698.999 |
| before  | q3_join_zone_lookup_top20 |       7 | 397.067 | 424.02  | 407.798    |  402.697 |
| before  | q4_payment_type_stats     |       7 | 230.213 | 267.429 | 246.018    |  244.208 |
| before  | q5_hourly_peak            |       7 | 728.757 | 748.457 | 737.023    |  736.645 |
| before  | q5_mart_hourly_peak       |       7 |   0.307 |   0.554 |   0.379857 |    0.36  |


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
