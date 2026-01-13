from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import psycopg


QUERIES: Dict[str, str] = {
    "q1_top_pickup_zones_day": """
        select
          pu_location_id,
          count(*) as trips
        from clean.clean_yellow_trips
        where pickup_ts >= timestamp '2024-01-31 00:00:00'
          and pickup_ts <  timestamp '2024-02-01 00:00:00'
        group by 1
        order by trips desc
        limit 20
    """,

    "q2_revenue_by_day": """
        select
          pickup_ts::date as trip_date,
          count(*) as trips,
          sum(total_amount) as revenue
        from clean.clean_yellow_trips
        group by 1
        order by 1
    """,

    # ✅ MART version of q2 (pre-aggregated table)
    "q2_mart_daily_revenue": """
        select
          trip_date,
          trips,
          revenue
        from marts.marts_daily_revenue
        order by 1
    """,

    "q3_join_zone_lookup_top20": """
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
    """,

    "q4_payment_type_stats": """
        select
          payment_type,
          count(*) as trips,
          avg(tip_amount) as avg_tip
        from clean.clean_yellow_trips
        group by 1
        order by trips desc
    """,

    "q5_hourly_peak": """
        select
          extract(hour from pickup_ts)::int as hr,
          count(*) as trips
        from clean.clean_yellow_trips
        group by 1
        order by trips desc
    """,

    # ✅ MART version of q5 (pre-aggregated by hour)
    "q5_mart_hourly_peak": """
        select
          hr,
          sum(trips) as trips
        from marts.marts_hourly_peak
        group by 1
        order by trips desc
    """,
}



def _pg_conn() -> psycopg.Connection:
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "nyc_taxi"),
        user=os.getenv("POSTGRES_USER", "nyc"),
        password=os.getenv("POSTGRES_PASSWORD", "nyc"),
        autocommit=True,
    )


def _run_and_drain(cur: psycopg.Cursor, sql: str) -> None:
    cur.execute(sql)
    # гарантируем, что запрос реально выполнился (а не “лениво”)
    if cur.description is not None:
        cur.fetchall()


def _pct(values: List[float], p: float) -> float:
    if not values:
        return float("nan")
    s = sorted(values)
    k = int(round((len(s) - 1) * p))
    return s[k]


def run_benchmarks(iters: int = 7, warmup: int = 1, phase: str = "after") -> Path:
    """
    Пишет:
      data/reports/benchmarks_<phase>_<timestamp>.csv
      data/reports/benchmarks_<phase>_<timestamp>.md
    """
    out_dir = Path("data") / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = out_dir / f"benchmarks_{phase}_{stamp}.csv"
    md_path = out_dir / f"benchmarks_{phase}_{stamp}.md"

    rows: List[dict] = []

    with _pg_conn() as conn:
        with conn.cursor() as cur:
            # чуть меньше шума от JIT/планировщика (не обязательно, но полезно)
            try:
                cur.execute("SET jit = off;")
            except Exception:
                pass

            for name, sql in QUERIES.items():
                sql = sql.strip().rstrip(";")

                # warmup (не считаем)
                for _ in range(max(0, warmup)):
                    _run_and_drain(cur, sql)

                times: List[float] = []
                for i in range(1, iters + 1):
                    t0 = time.perf_counter()
                    _run_and_drain(cur, sql)
                    dt_ms = (time.perf_counter() - t0) * 1000.0
                    times.append(dt_ms)

                    rows.append(
                        {
                            "phase": phase,
                            "query": name,
                            "iter": i,
                            "elapsed_ms": round(dt_ms, 3),
                        }
                    )

                print(
                    f"[bench] {name}: median={_pct(times, 0.5):.1f}ms "
                    f"p95={_pct(times, 0.95):.1f}ms min={min(times):.1f}ms max={max(times):.1f}ms"
                )

    df = pd.DataFrame(rows)
    df.to_csv(csv_path, index=False)

    summary = (
        df.groupby(["phase", "query"])["elapsed_ms"]
        .agg(["count", "min", "max", "mean", "median"])
        .reset_index()
    )

    # красивый markdown отчёт
    md_lines = []
    md_lines.append(f"# Benchmarks ({phase})\n")
    md_lines.append(f"Generated: `{stamp}`\n")
    md_lines.append(f"Runs per query: `{iters}` (warmup: `{warmup}`)\n")
    md_lines.append("## Summary (ms)\n")
    md_lines.append(summary.to_markdown(index=False))
    md_lines.append("\n\n## Queries\n")
    for name, sql in QUERIES.items():
        md_lines.append(f"### {name}\n")
        md_lines.append("```sql")
        md_lines.append(sql.strip().rstrip(";"))
        md_lines.append("```\n")

    md_path.write_text("\n".join(md_lines), encoding="utf-8")

    print(f"[bench] wrote: {csv_path}")
    print(f"[bench] wrote: {md_path}")

    return csv_path
