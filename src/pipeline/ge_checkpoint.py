from __future__ import annotations

import json
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import unquote, urlparse

import great_expectations as gx

# ---- compat import for ExpectationConfiguration (differs by GE versions) ----
ExpectationConfiguration = None  # type: ignore[assignment]
try:
    # some versions
    from great_expectations.core import ExpectationConfiguration as _EC  # type: ignore

    ExpectationConfiguration = _EC  # type: ignore[misc]
except Exception:
    try:
        # older versions
        from great_expectations.core.expectation_configuration import (  # type: ignore
            ExpectationConfiguration as _EC2,
        )

        ExpectationConfiguration = _EC2  # type: ignore[misc]
    except Exception:
        ExpectationConfiguration = None  # type: ignore[assignment]


def _pg_connection_string() -> str:
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "nyc_taxi")
    user = os.getenv("POSTGRES_USER", "nyc")
    pwd = os.getenv("POSTGRES_PASSWORD", "nyc")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


def _safe_json_payload(obj: Any) -> Dict[str, Any]:
    """Return a real dict; avoid strings with escaped '\\n'."""
    if isinstance(obj, dict):
        return obj

    if isinstance(obj, str):
        try:
            parsed = json.loads(obj)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {"text": obj}

    for meth in ("to_json_dict", "to_dict", "dict"):
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

    return {"text": str(obj)}


def _copy_data_docs_to_repo(urls: Dict[str, str], dest_root: Path) -> Optional[Path]:
    """Copy file:///tmp/... data docs into repo docs/ge/data_docs."""
    if not urls:
        return None

    url = urls.get("local_site") or next(iter(urls.values()))
    parsed = urlparse(url)
    if parsed.scheme != "file":
        return None

    index_path = Path(unquote(parsed.path))
    if not index_path.exists():
        return None

    src_dir = index_path.parent
    dest_dir = dest_root / "data_docs"

    if dest_dir.exists():
        shutil.rmtree(dest_dir, ignore_errors=True)
    shutil.copytree(src_dir, dest_dir)

    return dest_dir / "index.html"


def _get_or_add_datasource(context: gx.DataContext, name: str, conn_str: str):
    try:
        return context.data_sources.get(name)
    except Exception:
        return context.data_sources.add_postgres(name, connection_string=conn_str)


def _get_or_add_table_asset(data_source, asset_name: str, table_name: str, schema_name: str):
    """GE APIs differ. Try to get asset, otherwise add."""
    for getter in ("get_asset", "get_data_asset"):
        if hasattr(data_source, getter):
            try:
                return getattr(data_source, getter)(asset_name)
            except Exception:
                pass

    return data_source.add_table_asset(
        name=asset_name,
        table_name=table_name,
        schema_name=schema_name,
    )


def _get_or_add_batch_definition_whole_table(data_asset, bd_name: str):
    if hasattr(data_asset, "get_batch_definition"):
        try:
            return data_asset.get_batch_definition(bd_name)
        except Exception:
            pass
    return data_asset.add_batch_definition_whole_table(bd_name)


def _reset_suite_expectations(suite: Any) -> None:
    """Avoid duplicates: make suite deterministic each run."""
    if hasattr(suite, "expectations"):
        try:
            suite.expectations = []
            return
        except Exception:
            pass
    if hasattr(suite, "expectation_configurations"):
        try:
            suite.expectation_configurations = []
            return
        except Exception:
            pass


def _add_expectation_compat(
    suite: Any,
    expectation_type: str,
    kwargs: Dict[str, Any],
    meta: Dict[str, Any],
) -> None:
    """
    Add expectations in a version-tolerant way:
    1) via ExpectationConfiguration (if available)
    2) via gx.expectations classes (fallback)
    3) best-effort dict config (last resort)
    """
    # 1) Preferred: ExpectationConfiguration
    if ExpectationConfiguration is not None:
        try:
            suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type=expectation_type,
                    kwargs=kwargs,
                    meta=meta,
                )
            )
            return
        except Exception:
            pass

    # 2) Fallback: class-based expectations (new gx.expectations API)
    type_to_class = {
        "expect_column_pair_values_A_to_be_greater_than_B": "ExpectColumnPairValuesAToBeGreaterThanB",
        "expect_column_values_to_be_between": "ExpectColumnValuesToBeBetween",
        "expect_column_values_to_be_in_set": "ExpectColumnValuesToBeInSet",
    }
    cls_name = type_to_class.get(expectation_type)
    if cls_name and hasattr(gx.expectations, cls_name):
        cls = getattr(gx.expectations, cls_name)
        severity = meta.get("severity")
        try:
            if severity is not None:
                suite.add_expectation(cls(**kwargs, severity=severity))
            else:
                suite.add_expectation(cls(**kwargs))
            return
        except TypeError:
            # some versions don't accept severity
            try:
                suite.add_expectation(cls(**kwargs))
                return
            except Exception:
                pass
        except Exception:
            pass

    # 3) Last resort (won't crash pipeline)
    try:
        suite.add_expectation({"expectation_type": expectation_type, "kwargs": kwargs, "meta": meta})
    except Exception:
        pass


def _build_suite(context: gx.DataContext, suite_name: str) -> Any:
    """Create/update suite deterministically (professor-friendly reproducibility)."""
    try:
        suite = context.suites.get(suite_name)
    except Exception:
        suite = context.suites.add(gx.core.expectation_suite.ExpectationSuite(name=suite_name))

    _reset_suite_expectations(suite)

    # ---- core integrity (critical) ----
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="pickup_ts", severity="critical"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="dropoff_ts", severity="critical"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="pu_location_id", severity="critical"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="do_location_id", severity="critical"))

    # ---- sanity checks (warning) ----
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="trip_distance", min_value=0.0, severity="warning")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="total_amount", min_value=0.0, severity="warning")
    )

    # ---- thesis-style business rules ----
    mostly = float(os.getenv("GE_MOSTLY", "0.999"))

    # 1) dropoff_ts >= pickup_ts
    _add_expectation_compat(
        suite,
        "expect_column_pair_values_A_to_be_greater_than_B",
        kwargs={
            "column_A": "dropoff_ts",
            "column_B": "pickup_ts",
            "or_equal": True,
            "ignore_row_if": "either_value_is_missing",
            "mostly": mostly,
        },
        meta={
            "severity": "critical",
            "notes": "Business rule: dropoff_ts must be greater than or equal to pickup_ts.",
        },
    )

    # 2) passenger_count in [0, 8]
    _add_expectation_compat(
        suite,
        "expect_column_values_to_be_between",
        kwargs={
            "column": "passenger_count",
            "min_value": 0,
            "max_value": 8,
            "mostly": mostly,
        },
        meta={
            "severity": "warning",
            "notes": "Sanity check: passenger_count should be within [0, 8].",
        },
    )

    payment_mostly = float(os.getenv("GE_PAYMENT_TYPE_MOSTLY", "0.95"))

    _add_expectation_compat(
        suite,
        "expect_column_values_to_be_in_set",
        kwargs={
            "column": "payment_type",
            "value_set": [1, 2, 3, 4, 5, 6],
            "mostly": payment_mostly,
        },
        meta={
            "severity": "warning",
            "notes": (
                "Sanity check: payment_type should be one of TLC codes {1..6}. "
                "Some rows may contain 0 (unknown-coded); tolerated via GE_PAYMENT_TYPE_MOSTLY."
            ),
        },
    )


    return suite


def _get_or_add_validation_definition(context: gx.DataContext, name: str, batch_definition: Any, suite: Any):
    try:
        return context.validation_definitions.get(name)
    except Exception:
        return context.validation_definitions.add(
            gx.core.validation_definition.ValidationDefinition(
                name=name,
                data=batch_definition,
                suite=suite,
            )
        )


def _get_or_add_checkpoint(context: gx.DataContext, name: str, validation_definition: Any):
    try:
        return context.checkpoints.get(name)
    except Exception:
        return context.checkpoints.add(
            gx.checkpoint.checkpoint.Checkpoint(
                name=name,
                validation_definitions=[validation_definition],
            )
        )


def main() -> None:
    suite_name = os.getenv("GE_SUITE_NAME", "suite_clean_yellow_trips")
    checkpoint_name = os.getenv("GE_CHECKPOINT_NAME", "cp_clean_yellow_trips")
    validation_def_name = os.getenv("GE_VALIDATION_DEF_NAME", "vd_clean_yellow_trips_whole_table")
    fail_on_error = os.getenv("GE_FAIL_ON_ERROR", "1") == "1"

    repo_docs_dir = Path("docs") / "ge"
    repo_docs_dir.mkdir(parents=True, exist_ok=True)

    out_dir = Path("data") / "reports" / "ge"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 1) Context
    context = gx.get_context()

    # 2) Datasource / Asset / Batch
    conn_str = _pg_connection_string()
    data_source = _get_or_add_datasource(context, "nyc_postgres", conn_str)

    data_asset = _get_or_add_table_asset(
        data_source,
        asset_name="clean_yellow_trips",
        table_name="clean_yellow_trips",
        schema_name="clean",
    )

    batch_definition = _get_or_add_batch_definition_whole_table(data_asset, "whole_table")

    # 3) Suite
    suite = _build_suite(context, suite_name)

    # 4) Validation Definition
    validation_definition = _get_or_add_validation_definition(context, validation_def_name, batch_definition, suite)

    # 5) Checkpoint
    checkpoint = _get_or_add_checkpoint(context, checkpoint_name, validation_definition)

    # 6) Run
    result = checkpoint.run()

    # 7) Data Docs -> copy into repo
    try:
        urls = context.build_data_docs()
        print("[ge] data docs:", urls)
        copied_index = _copy_data_docs_to_repo(urls, repo_docs_dir)
        if copied_index:
            print(f"[ge] data docs copied to: {copied_index.as_posix()}")
            print("[ge] open on host:", copied_index.as_posix())
    except Exception as e:
        print("[ge] data docs build/copy skipped:", e)

    # 8) Save checkpoint result (pretty JSON)
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
