# Benchmarks (after)

Generated: `20260120_145535`

Runs per query: `7` (warmup: `1`)

## Summary (ms)

| phase   | query                     |   count |     min |     max |       mean |   median |
|:--------|:--------------------------|--------:|--------:|--------:|-----------:|---------:|
| after   | q1_top_pickup_zones_day   |       7 |  35.014 |  36.502 |  35.6249   |   35.427 |
| after   | q2_mart_daily_revenue     |       7 |   0.237 |   0.406 |   0.307143 |    0.293 |
| after   | q2_revenue_by_day         |       7 | 718.066 | 724.948 | 721.162    |  720.502 |
| after   | q3_join_zone_lookup_top20 |       7 | 394.97  | 531.882 | 434.498    |  400.344 |
| after   | q4_payment_type_stats     |       7 | 241.449 | 284.36  | 260.895    |  261.049 |
| after   | q5_hourly_peak            |       7 | 633.651 | 645.169 | 638.239    |  637.705 |
| after   | q5_mart_hourly_peak       |       7 |   0.289 |   0.534 |   0.389    |    0.344 |


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
