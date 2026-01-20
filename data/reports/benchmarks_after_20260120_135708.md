# Benchmarks (after)

Generated: `20260120_135708`

Runs per query: `7` (warmup: `1`)

## Summary (ms)

| phase   | query                     |   count |      min |      max |        mean |   median |
|:--------|:--------------------------|--------:|---------:|---------:|------------:|---------:|
| after   | q1_top_pickup_zones_day   |       7 |  583.617 |  712.644 |  626.468    |  604.399 |
| after   | q2_mart_daily_revenue     |       7 |    0.388 |    0.791 |    0.483714 |    0.448 |
| after   | q2_revenue_by_day         |       7 | 1581.35  | 1900.6   | 1730.49     | 1765.37  |
| after   | q3_join_zone_lookup_top20 |       7 | 2057.32  | 2599.29  | 2242.37     | 2249.73  |
| after   | q4_payment_type_stats     |       7 | 1281.33  | 1560.94  | 1361.31     | 1323.65  |
| after   | q5_hourly_peak            |       7 | 3216.02  | 3942.32  | 3658.75     | 3612.12  |
| after   | q5_mart_hourly_peak       |       7 |    0.426 |    1.3   |    0.629143 |    0.513 |


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
