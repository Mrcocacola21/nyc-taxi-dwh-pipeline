import argparse
import os
from dotenv import load_dotenv

from src.pipeline.ingest import ingest_all
from src.pipeline.benchmarks import run_benchmarks
from src.pipeline.bench_compare import compare_latest_reports



def main():
    load_dotenv()

    p = argparse.ArgumentParser(prog="pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)
    bench = sub.add_parser("bench", help="Run SQL benchmarks on clean layer")
    bench.add_argument("--iters", type=int, default=7)
    bench.add_argument("--warmup", type=int, default=1)
    bench.add_argument("--phase", default="after", choices=["before", "after"])
    cmp = sub.add_parser("bench-compare", help="Compare latest before/after benchmark CSVs and write speedup report")


    ing = sub.add_parser("ingest", help="Download and load TLC data into raw schema")
    ing.add_argument("--months", default=os.getenv("TAXI_MONTHS", "2024-01"),
                     help="Comma-separated months, e.g. 2024-01,2024-02")
    ing.add_argument("--dataset", default="yellow", choices=["yellow"],
                     help="Which TLC dataset to ingest")

    args = p.parse_args()

    if args.cmd == "ingest":
        months = [m.strip() for m in args.months.split(",") if m.strip()]
        ingest_all(months=months, dataset=args.dataset)
    if args.cmd == "bench":
        run_benchmarks(iters=args.iters, warmup=args.warmup, phase=args.phase)
    if args.cmd == "bench-compare":
        compare_latest_reports()


if __name__ == "__main__":
    main()
