# Benchmarks (after)

Generated: `20260110_134946`

Runs per query: `7` (warmup: `1`)

## Summary (ms)

| phase   | query                     |   count |     min |     max |     mean |   median |
|:--------|:--------------------------|--------:|--------:|--------:|---------:|---------:|
| after   | q1_top_pickup_zones_day   |       7 |  35.509 |  37.751 |  36.6487 |   36.853 |
| after   | q2_revenue_by_day         |       7 | 705.878 | 726.568 | 714.873  |  714.029 |
| after   | q3_join_zone_lookup_top20 |       7 | 396.934 | 519.993 | 432.237  |  414.343 |
| after   | q4_payment_type_stats     |       7 | 230.513 | 243.94  | 236.89   |  233.858 |
| after   | q5_hourly_peak            |       7 | 629.653 | 659.174 | 642.788  |  645.887 |


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
