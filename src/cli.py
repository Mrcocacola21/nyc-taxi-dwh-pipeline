import argparse
import json
import os
import shlex
import subprocess
import sys
from dotenv import load_dotenv

from src.pipeline.ingest import ingest_all
from src.pipeline.benchmarks import run_benchmarks
from src.pipeline.bench_compare import compare_latest_reports


def _run_shell(cmd: str) -> None:
    """Run a shell command (inside container) and fail fast with clear logging."""
    print(f"\n[run-all] $ {cmd}\n", flush=True)
    subprocess.run(cmd, shell=True, check=True)


def _parse_csv_list(raw: str) -> list[str]:
    return [v.strip() for v in raw.split(",") if v.strip()]


def main():
    load_dotenv()

    p = argparse.ArgumentParser(prog="pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)

    # -------- bench --------
    bench = sub.add_parser("bench", help="Run SQL benchmarks on clean layer")
    bench.add_argument("--iters", type=int, default=7)
    bench.add_argument("--warmup", type=int, default=1)
    bench.add_argument("--phase", default="after", choices=["before", "after"])
    bench.add_argument(
        "--run-id",
        default="",
        help="Optional run id; use the same value for before/after pair comparisons",
    )
    bench.add_argument(
        "--batches",
        default="",
        help="Optional comma-separated batch ids for metadata, e.g. 2024-01,2024-02",
    )

    # -------- bench-compare --------
    bench_compare = sub.add_parser(
        "bench-compare",
        help="Compare benchmark CSVs deterministically by run-id or explicit file pair",
    )
    bench_compare.add_argument("--run-id", default="", help="Compare benchmarks_<run_id>_before/after.csv")
    bench_compare.add_argument("--before-file", default="", help="Explicit before CSV path")
    bench_compare.add_argument("--after-file", default="", help="Explicit after CSV path")
    bench_compare.add_argument(
        "--allow-mismatched-runs",
        action="store_true",
        help="Allow explicit before/after files with different run_id",
    )

    # -------- ingest --------
    ing = sub.add_parser("ingest", help="Download and load TLC data into raw schema")
    ing.add_argument(
        "--months",
        default=os.getenv("TAXI_MONTHS", "2024-01"),
        help="Comma-separated months, e.g. 2024-01,2024-02",
    )
    ing.add_argument(
        "--dataset",
        default="yellow",
        choices=["yellow"],
        help="Which TLC dataset to ingest",
    )
    ing.add_argument(
        "--replace-batch",
        action="store_true",
        help="If batch_id already exists in raw tables: delete it and re-ingest",
    )

    # -------- run-all --------
    runall = sub.add_parser(
        "run-all",
        help="One-shot run: ingest -> dbt run -> dbt test -> GE checkpoint -> benchmarks",
    )
    runall.add_argument(
        "--months",
        default=os.getenv("TAXI_MONTHS", "2024-01"),
        help="Comma-separated months, e.g. 2024-01,2024-02",
    )
    runall.add_argument(
        "--dataset",
        default="yellow",
        choices=["yellow"],
        help="Which TLC dataset to ingest",
    )
    runall.add_argument("--phase", default="after", choices=["before", "after"])
    runall.add_argument(
        "--run-id",
        default="",
        help="Optional run id for benchmark outputs in this run",
    )
    runall.add_argument("--iters", type=int, default=7)
    runall.add_argument("--warmup", type=int, default=1)

    runall.add_argument("--full-refresh", action="store_true", help="Run dbt with --full-refresh")
    runall.add_argument(
        "--dbt-select",
        default="",
        help="Optional dbt selector, e.g. 'marts' or 'tag:core'",
    )

    runall.add_argument("--skip-ingest", action="store_true")
    runall.add_argument("--skip-dbt", action="store_true")
    runall.add_argument("--skip-dbt-test", action="store_true")
    runall.add_argument("--skip-ge", action="store_true")
    runall.add_argument("--skip-bench", action="store_true")
    runall.add_argument(
        "--replace-batch",
        action="store_true",
        help="If batch_id already exists in raw tables: delete it and re-ingest",
    )

    args = p.parse_args()

    if args.cmd == "ingest":
        months = _parse_csv_list(args.months)
        ingest_all(months=months, dataset=args.dataset, replace_batch=args.replace_batch)
        return

    if args.cmd == "bench":
        run_benchmarks(
            iters=args.iters,
            warmup=args.warmup,
            phase=args.phase,
            run_id=args.run_id or None,
            batch_ids=_parse_csv_list(args.batches),
        )
        return

    if args.cmd == "bench-compare":
        try:
            compare_latest_reports(
                run_id=args.run_id,
                before_file=args.before_file,
                after_file=args.after_file,
                allow_mismatched_runs=args.allow_mismatched_runs,
            )
        except (FileNotFoundError, ValueError) as e:
            print(f"[bench-compare] ERROR: {e}", file=sys.stderr)
            sys.exit(2)
        return

    if args.cmd == "run-all":
        months = _parse_csv_list(args.months)
        if not months:
            print("[run-all] ERROR: --months is empty", file=sys.stderr)
            sys.exit(2)

        if not args.skip_ingest:
            ingest_all(months=months, dataset=args.dataset, replace_batch=args.replace_batch)

        if not args.skip_dbt:
            dbt_cmd = "cd /app/dbt && dbt run"
            if args.full_refresh:
                dbt_cmd += " --full-refresh"
            if args.replace_batch:
                dbt_vars = shlex.quote(json.dumps({"batch_ids": months}))
                dbt_cmd += f" --vars {dbt_vars}"
            if args.dbt_select.strip():
                dbt_cmd += f" --select {args.dbt_select.strip()}"
            _run_shell(dbt_cmd)

        if not args.skip_dbt_test:
            _run_shell("cd /app/dbt && dbt test")

        if not args.skip_ge:
            _run_shell("python -m src.pipeline.ge_checkpoint")

        if not args.skip_bench:
            run_benchmarks(
                iters=args.iters,
                warmup=args.warmup,
                phase=args.phase,
                run_id=args.run_id or None,
                batch_ids=months,
            )

        print("\n[run-all] ✅ DONE\n", flush=True)
        return


if __name__ == "__main__":
    main()
