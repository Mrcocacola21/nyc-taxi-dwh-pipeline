from __future__ import annotations

import json
import os
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

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
    if cur.description is not None:
        cur.fetchall()


def _pct(values: List[float], p: float) -> float:
    if not values:
        return float("nan")
    s = sorted(values)
    k = int(round((len(s) - 1) * p))
    return s[k]


def _normalize_batch_ids(batch_ids: Optional[List[str]]) -> List[str]:
    if not batch_ids:
        return []
    out: List[str] = []
    for batch_id in batch_ids:
        v = str(batch_id).strip()
        if v:
            out.append(v)
    return sorted(set(out))


def _generate_run_id() -> str:
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")


def _utc_now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _git_sha() -> Optional[str]:
    try:
        raw = subprocess.check_output(["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL)
    except Exception:
        return None
    sha = raw.decode("utf-8", errors="ignore").strip()
    return sha or None


def _safe_count(cur: psycopg.Cursor, relation: str) -> Optional[int]:
    try:
        cur.execute(f"select count(*) from {relation}")
        row = cur.fetchone()
        if row is None:
            return None
        return int(row[0])
    except Exception:
        return None


def _safe_distinct_batch_ids(cur: psycopg.Cursor, relation: str) -> List[str]:
    try:
        cur.execute(f"select distinct batch_id::text from {relation} where batch_id is not null order by 1")
        return [str(r[0]) for r in cur.fetchall()]
    except Exception:
        return []


def _load_meta(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _write_meta(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def run_benchmarks(
    iters: int = 7,
    warmup: int = 1,
    phase: str = "after",
    run_id: Optional[str] = None,
    batch_ids: Optional[List[str]] = None,
) -> Path:
    """
    Writes:
      data/reports/benchmarks_<run_id>_<phase>.csv
      data/reports/benchmarks_<run_id>_<phase>.md
      data/reports/bench_meta_<run_id>.json
    """
    normalized_phase = phase.strip().lower()
    if normalized_phase not in {"before", "after"}:
        raise ValueError("phase must be 'before' or 'after'")

    normalized_run_id = (run_id or "").strip() or _generate_run_id()
    normalized_batch_ids = _normalize_batch_ids(batch_ids)
    created_at = _utc_now()

    out_dir = Path("data") / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_path = out_dir / f"benchmarks_{normalized_run_id}_{normalized_phase}.csv"
    md_path = out_dir / f"benchmarks_{normalized_run_id}_{normalized_phase}.md"
    meta_path = out_dir / f"bench_meta_{normalized_run_id}.json"

    rows: List[dict] = []
    row_counts: Dict[str, Optional[int]] = {}
    discovered_batches: Dict[str, List[str]] = {"raw": [], "clean": []}

    with _pg_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("SET jit = off;")
            except Exception:
                pass

            for name, sql in QUERIES.items():
                sql = sql.strip().rstrip(";")

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
                            "run_id": normalized_run_id,
                            "phase": normalized_phase,
                            "query": name,
                            "iter": i,
                            "elapsed_ms": round(dt_ms, 3),
                        }
                    )

                print(
                    f"[bench] {name}: median={_pct(times, 0.5):.1f}ms "
                    f"p95={_pct(times, 0.95):.1f}ms min={min(times):.1f}ms max={max(times):.1f}ms"
                )

            row_counts = {
                "raw.yellow_trips": _safe_count(cur, "raw.yellow_trips"),
                "stg.stg_yellow_trips": _safe_count(cur, "stg.stg_yellow_trips"),
                "clean.clean_yellow_trips": _safe_count(cur, "clean.clean_yellow_trips"),
                "quarantine.quarantine_yellow_trips": _safe_count(cur, "quarantine.quarantine_yellow_trips"),
                "marts.marts_daily_revenue": _safe_count(cur, "marts.marts_daily_revenue"),
                "marts.marts_hourly_peak": _safe_count(cur, "marts.marts_hourly_peak"),
            }
            discovered_batches = {
                "raw": _safe_distinct_batch_ids(cur, "raw.yellow_trips"),
                "clean": _safe_distinct_batch_ids(cur, "clean.clean_yellow_trips"),
            }

    df = pd.DataFrame(rows)
    df.to_csv(csv_path, index=False)

    summary = (
        df.groupby(["run_id", "phase", "query"])["elapsed_ms"]
        .agg(["count", "min", "max", "mean", "median"])
        .reset_index()
    )

    md_lines: List[str] = []
    md_lines.append(f"# Benchmarks ({normalized_phase})\n")
    md_lines.append(f"Run ID: `{normalized_run_id}`\n")
    md_lines.append(f"Generated (UTC): `{created_at}`\n")
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

    git_sha = _git_sha()
    meta = _load_meta(meta_path)
    phases_meta = meta.get("phases")
    if not isinstance(phases_meta, dict):
        phases_meta = {}

    phases_meta[normalized_phase] = {
        "phase": normalized_phase,
        "index_state": normalized_phase,
        "created_at": created_at,
        "csv_file": csv_path.as_posix(),
        "md_file": md_path.as_posix(),
        "iters": iters,
        "warmup": warmup,
    }

    payload: Dict[str, Any] = {
        "run_id": normalized_run_id,
        "git_sha": meta.get("git_sha") or git_sha,
        "created_at": meta.get("created_at") or created_at,
        "last_updated_at": created_at,
        "phase": normalized_phase,
        "index_state": normalized_phase,
        "command_args": {"iters": iters, "warmup": warmup},
        "requested_batches": normalized_batch_ids,
        "discovered_batches": discovered_batches,
        "row_counts": row_counts,
        "phases": phases_meta,
    }
    _write_meta(meta_path, payload)

    print(f"[bench] run_id: {normalized_run_id}")
    print(f"[bench] wrote: {csv_path}")
    print(f"[bench] wrote: {md_path}")
    print(f"[bench] wrote: {meta_path}")

    return csv_path
