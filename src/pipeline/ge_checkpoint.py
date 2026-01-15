from __future__ import annotations

import json
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse, unquote

import great_expectations as gx


def _pg_connection_string() -> str:
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "nyc_taxi")
    user = os.getenv("POSTGRES_USER", "nyc")
    pwd = os.getenv("POSTGRES_PASSWORD", "nyc")
    # GE uses sqlalchemy url (psycopg2 driver) for postgres datasource
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


def _safe_json_payload(obj: Any) -> Dict[str, Any]:
    """
    GE result objects differ by versions.
    We try to produce a real dict (not a string with escaped \\n).
    """
    # 1) direct dict-like
    if isinstance(obj, dict):
        return obj

    # 2) sometimes describe() returns a JSON string
    if isinstance(obj, str):
        try:
            parsed = json.loads(obj)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {"text": obj}

    # 3) try common methods
    for meth in ("to_json_dict", "to_dict"):
        if hasattr(obj, meth):
            try:
                val = getattr(obj, meth)()
                if isinstance(val, dict):
                    return val
                if isinstance(val, str):
                    try:
                        parsed = json.loads(val)
                        if isinstance(parsed, dict):
                            return parsed
                    except Exception:
                        return {"text": val}
            except Exception:
                pass

    # 4) fallback: string repr
    return {"text": str(obj)}


def _copy_data_docs_to_repo(urls: Dict[str, str], dest_root: Path) -> Optional[Path]:
    """
    build_data_docs() often returns a file:///tmp/.../index.html.
    We copy that site folder into repo docs/ge/data_docs so it's accessible from host.
    """
    if not urls:
        return None

    # prefer "local_site" if present, otherwise take first
    url = urls.get("local_site") or next(iter(urls.values()))
    parsed = urlparse(url)
    if parsed.scheme != "file":
        return None

    index_path = Path(unquote(parsed.path))
    if not index_path.exists():
        return None

    src_dir = index_path.parent
    dest_dir = dest_root / "data_docs"

    # replace old docs
    if dest_dir.exists():
        shutil.rmtree(dest_dir, ignore_errors=True)
    shutil.copytree(src_dir, dest_dir)

    return dest_dir / "index.html"


def main() -> None:
    # ---- config knobs (easy to extend later) ----
    suite_name = os.getenv("GE_SUITE_NAME", "suite_clean_yellow_trips")
    checkpoint_name = os.getenv("GE_CHECKPOINT_NAME", "cp_clean_yellow_trips")
    fail_on_error = os.getenv("GE_FAIL_ON_ERROR", "1") == "1"

    # where to store artifacts in repo
    repo_docs_dir = Path("docs") / "ge"
    repo_docs_dir.mkdir(parents=True, exist_ok=True)

    out_dir = Path("data") / "reports" / "ge"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 1) Data Context (ephemeral is OK; we handle it)
    context = gx.get_context()

    # 2) Datasource / Asset / BatchDefinition
    connection_string = _pg_connection_string()

    # If datasource already exists (some GE versions persist), reuse it
    try:
        data_source = context.data_sources.get("nyc_postgres")
    except Exception:
        data_source = context.data_sources.add_postgres(
            "nyc_postgres", connection_string=connection_string
        )

    data_asset = data_source.add_table_asset(
        name="clean_yellow_trips",
        table_name="clean_yellow_trips",
        schema_name="clean",
    )
    batch_definition = data_asset.add_batch_definition_whole_table("whole_table")

    # 3) Expectation Suite (idempotent-ish)
    try:
        suite = context.suites.get(suite_name)
    except Exception:
        suite = context.suites.add(
            gx.core.expectation_suite.ExpectationSuite(name=suite_name)
        )

        # critical not-null checks
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="pickup_ts", severity="critical"
            )
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="dropoff_ts", severity="critical"
            )
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="pu_location_id", severity="critical"
            )
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="do_location_id", severity="critical"
            )
        )

        # warning sanity checks (soft rules)
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="trip_distance", min_value=0.0, severity="warning"
            )
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="total_amount", min_value=0.0, severity="warning"
            )
        )

    # 4) Validation Definition
    validation_definition = context.validation_definitions.add(
        gx.core.validation_definition.ValidationDefinition(
            name="vd_clean_yellow_trips_whole_table",
            data=batch_definition,
            suite=suite,
        )
    )

    # 5) Checkpoint
    checkpoint = context.checkpoints.add(
        gx.checkpoint.checkpoint.Checkpoint(
            name=checkpoint_name,
            validation_definitions=[validation_definition],
        )
    )

    # 6) Run
    result = checkpoint.run()

    # 7) Build docs (ephemeral builds to /tmp/...), then copy into repo/docs
    try:
        urls = context.build_data_docs()
        print("[ge] data docs:", urls)
        copied_index = _copy_data_docs_to_repo(urls, repo_docs_dir)
        if copied_index:
            print(f"[ge] data docs copied to: {copied_index.as_posix()}")
            print("[ge] open in browser on host: file://" + copied_index.resolve().as_posix())
    except Exception as e:
        print("[ge] data docs build/copy skipped:", e)

    # 8) Save checkpoint result nicely (real JSON, no '\\n' escapes)
    # Prefer describe() if available
    described = result.describe() if hasattr(result, "describe") else result
    payload = _safe_json_payload(described)

    out_path = out_dir / f"checkpoint_result_{stamp}.json"
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(f"[ge] wrote: {out_path}")

    # 9) Console summary
    print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))

    # 10) Fail pipeline if desired
    success = bool(payload.get("success", True))
    if fail_on_error and not success:
        print("[ge] validation FAILED (GE_FAIL_ON_ERROR=1). Exiting with code 1.")
        sys.exit(1)


if __name__ == "__main__":
    main()
