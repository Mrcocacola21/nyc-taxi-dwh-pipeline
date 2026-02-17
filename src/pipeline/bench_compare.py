from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd


REPORT_RE = re.compile(r"^benchmarks_(?P<run_id>.+)_(?P<phase>before|after)\.csv$")


def _parse_report_name(path: Path) -> Tuple[Optional[str], Optional[str]]:
    m = REPORT_RE.match(path.name)
    if not m:
        return None, None
    return m.group("run_id"), m.group("phase")


def _latest_complete_run(out_dir: Path) -> str:
    runs: dict[str, set[str]] = {}
    for path in out_dir.glob("benchmarks_*_*.csv"):
        run_id, phase = _parse_report_name(path)
        if not run_id or phase not in {"before", "after"}:
            continue
        runs.setdefault(run_id, set()).add(phase)

    complete = sorted(run_id for run_id, phases in runs.items() if {"before", "after"}.issubset(phases))
    if not complete:
        raise FileNotFoundError(
            "No complete benchmark run found. Expected both benchmarks_<run_id>_before.csv and benchmarks_<run_id>_after.csv"
        )
    return complete[-1]


def _resolve_compare_inputs(
    out_dir: Path,
    run_id: str,
    before_file: str,
    after_file: str,
    allow_mismatched_runs: bool,
) -> tuple[Path, Path, Optional[str]]:
    run_id = run_id.strip()
    before_file = before_file.strip()
    after_file = after_file.strip()

    if run_id:
        before = Path(before_file) if before_file else out_dir / f"benchmarks_{run_id}_before.csv"
        after = Path(after_file) if after_file else out_dir / f"benchmarks_{run_id}_after.csv"

        if not before.exists():
            raise FileNotFoundError(f"before file not found for run_id={run_id}: {before}")
        if not after.exists():
            raise FileNotFoundError(f"after file not found for run_id={run_id}: {after}")

        parsed_before_run_id, parsed_before_phase = _parse_report_name(before)
        parsed_after_run_id, parsed_after_phase = _parse_report_name(after)

        if parsed_before_phase != "before":
            raise ValueError(f"Expected a *_before.csv file, got: {before.name}")
        if parsed_after_phase != "after":
            raise ValueError(f"Expected a *_after.csv file, got: {after.name}")

        if parsed_before_run_id != run_id or parsed_after_run_id != run_id:
            raise ValueError("Provided files do not match the requested --run-id")

        return before, after, run_id

    if before_file or after_file:
        if not before_file or not after_file:
            raise ValueError("Provide both --before-file and --after-file")

        before = Path(before_file)
        after = Path(after_file)

        if not before.exists():
            raise FileNotFoundError(f"before file not found: {before}")
        if not after.exists():
            raise FileNotFoundError(f"after file not found: {after}")

        parsed_before_run_id, parsed_before_phase = _parse_report_name(before)
        parsed_after_run_id, parsed_after_phase = _parse_report_name(after)

        if parsed_before_phase != "before":
            raise ValueError(f"Expected a *_before.csv file, got: {before.name}")
        if parsed_after_phase != "after":
            raise ValueError(f"Expected a *_after.csv file, got: {after.name}")

        if not allow_mismatched_runs:
            if not parsed_before_run_id or not parsed_after_run_id:
                raise ValueError(
                    "Cannot infer run_id from file names. Use --allow-mismatched-runs to bypass this check explicitly."
                )
            if parsed_before_run_id != parsed_after_run_id:
                raise ValueError(
                    "Mismatched run_id between before/after files. "
                    "Use --allow-mismatched-runs only when this is intentional."
                )

        matched_run_id = parsed_before_run_id if parsed_before_run_id == parsed_after_run_id else None
        return before, after, matched_run_id

    auto_run_id = _latest_complete_run(out_dir)
    return (
        out_dir / f"benchmarks_{auto_run_id}_before.csv",
        out_dir / f"benchmarks_{auto_run_id}_after.csv",
        auto_run_id,
    )


def compare_latest_reports(
    run_id: str = "",
    before_file: str = "",
    after_file: str = "",
    allow_mismatched_runs: bool = False,
) -> Path:
    """
    Compare benchmark reports deterministically.

    Resolution order:
    1) --run-id (preferred): compares benchmarks_<run_id>_before/after.csv
    2) explicit --before-file and --after-file
    3) latest complete run_id that has both before+after files
    """
    out_dir = Path("data/reports")
    before, after, matched_run_id = _resolve_compare_inputs(
        out_dir=out_dir,
        run_id=run_id,
        before_file=before_file,
        after_file=after_file,
        allow_mismatched_runs=allow_mismatched_runs,
    )

    df_b = pd.read_csv(before)
    df_a = pd.read_csv(after)

    b = df_b.groupby("query")["elapsed_ms"].median().rename("before_ms")
    a = df_a.groupby("query")["elapsed_ms"].median().rename("after_ms")
    out = pd.concat([b, a], axis=1).reset_index()

    out["speedup_x"] = (out["before_ms"] / out["after_ms"]).round(2)
    out["improvement_pct"] = ((1 - (out["after_ms"] / out["before_ms"])) * 100).round(1)
    out = out.sort_values("speedup_x", ascending=False)

    stamp = matched_run_id or datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    md_path = out_dir / f"benchmarks_speedup_{stamp}.md"

    lines = []
    lines.append("# Benchmarks (before vs after)\n")
    lines.append(f"Run ID: `{matched_run_id or 'mixed'}`\n")
    lines.append(f"Before: `{before.name}`  \nAfter: `{after.name}`\n")
    lines.append("Median elapsed time per query (ms).\n")
    lines.append("| query | before_ms | after_ms | speedup_x | improvement_pct |")
    lines.append("|---|---:|---:|---:|---:|")
    for _, r in out.iterrows():
        lines.append(
            f"| {r['query']} | {r['before_ms']:.1f} | {r['after_ms']:.1f} | {r['speedup_x']:.2f} | {r['improvement_pct']:.1f}% |"
        )

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[bench-compare] wrote: {md_path}")
    return md_path
