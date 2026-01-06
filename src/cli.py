import argparse
import os
from dotenv import load_dotenv

from src.pipeline.ingest import ingest_all

def main():
    load_dotenv()

    p = argparse.ArgumentParser(prog="pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)

    ing = sub.add_parser("ingest", help="Download and load TLC data into raw schema")
    ing.add_argument("--months", default=os.getenv("TAXI_MONTHS", "2024-01"),
                     help="Comma-separated months, e.g. 2024-01,2024-02")
    ing.add_argument("--dataset", default="yellow", choices=["yellow"],
                     help="Which TLC dataset to ingest")

    args = p.parse_args()

    if args.cmd == "ingest":
        months = [m.strip() for m in args.months.split(",") if m.strip()]
        ingest_all(months=months, dataset=args.dataset)

if __name__ == "__main__":
    main()
