from __future__ import annotations

import glob
from pathlib import Path
import pandas as pd


def _latest(pattern: str) -> Path:
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No files matched: {pattern}")
    return Path(files[-1])


def compare_latest_reports() -> Path:
    """
    Берёт последние:
      data/reports/benchmarks_before_*.csv
      data/reports/benchmarks_after_*.csv
    Пишет:
      data/reports/benchmarks_speedup_*.md
    """
    before = _latest("data/reports/benchmarks_before_*.csv")
    after = _latest("data/reports/benchmarks_after_*.csv")

    df_b = pd.read_csv(before)
    df_a = pd.read_csv(after)

    # summary median per query
    b = df_b.groupby("query")["elapsed_ms"].median().rename("before_ms")
    a = df_a.groupby("query")["elapsed_ms"].median().rename("after_ms")
    out = pd.concat([b, a], axis=1).reset_index()

    out["speedup_x"] = (out["before_ms"] / out["after_ms"]).round(2)
    out["improvement_pct"] = ((1 - (out["after_ms"] / out["before_ms"])) * 100).round(1)

    # nice ordering
    out = out.sort_values("speedup_x", ascending=False)

    stamp = after.stem.replace("benchmarks_after_", "")
    md_path = Path("data/reports") / f"benchmarks_speedup_{stamp}.md"

    lines = []
    lines.append("# Benchmarks (before vs after)\n")
    lines.append(f"Before: `{before.name}`  \nAfter: `{after.name}`\n")
    lines.append("Median elapsed time per query (ms).\n")

    # markdown table without tabulate dependency: use manual markdown
    lines.append("| query | before_ms | after_ms | speedup_x | improvement_pct |")
    lines.append("|---|---:|---:|---:|---:|")
    for _, r in out.iterrows():
        lines.append(
            f"| {r['query']} | {r['before_ms']:.1f} | {r['after_ms']:.1f} | {r['speedup_x']:.2f} | {r['improvement_pct']:.1f}% |"
        )

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[bench-compare] wrote: {md_path}")
    return md_path
