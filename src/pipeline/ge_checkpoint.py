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


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _normalize_suite_version(raw: str, default: str = "v1") -> str:
    cleaned = (raw or default).strip()
    if not cleaned:
        cleaned = default
    return cleaned if cleaned.startswith("v") else f"v{cleaned}"


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
        "expect_column_values_to_not_be_null": "ExpectColumnValuesToNotBeNull",
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


def _add_policy_expectation(
    suite: Any,
    expectation_type: str,
    kwargs: Dict[str, Any],
    *,
    severity: str,
    domain: str,
    rationale: str,
    suite_version: str,
) -> None:
    _add_expectation_compat(
        suite,
        expectation_type,
        kwargs=kwargs,
        meta={
            "severity": severity,
            "domain": domain,
            "rationale": rationale,
            "suite_version": suite_version,
        },
    )


def _build_suite(
    context: gx.DataContext,
    suite_name: str,
    policy: str,
    suite_version: str,
    mostly: float,
    payment_mostly: float,
) -> Any:
    """
    Create/update one expectation suite deterministically.
    To create a new immutable policy version, increment GE_SUITE_VERSION_* to v2, etc.
    """
    try:
        suite = context.suites.get(suite_name)
    except Exception:
        suite = context.suites.add(gx.core.expectation_suite.ExpectationSuite(name=suite_name))

    _reset_suite_expectations(suite)

    if policy == "critical":
        _add_policy_expectation(
            suite,
            "expect_column_values_to_not_be_null",
            kwargs={"column": "pickup_ts"},
            severity="critical",
            domain="integrity",
            rationale="pickup_ts is required for lineage, time filters, and model joins.",
            suite_version=suite_version,
        )
        _add_policy_expectation(
            suite,
            "expect_column_values_to_not_be_null",
            kwargs={"column": "dropoff_ts"},
            severity="critical",
            domain="integrity",
            rationale="dropoff_ts is required for trip duration and temporal consistency checks.",
            suite_version=suite_version,
        )
        _add_policy_expectation(
            suite,
            "expect_column_values_to_not_be_null",
            kwargs={"column": "pu_location_id"},
            severity="critical",
            domain="integrity",
            rationale="pickup location id is required for geo marts and zone-based aggregations.",
            suite_version=suite_version,
        )
        _add_policy_expectation(
            suite,
            "expect_column_values_to_not_be_null",
            kwargs={"column": "do_location_id"},
            severity="critical",
            domain="integrity",
            rationale="dropoff location id is required for geo marts and zone-based aggregations.",
            suite_version=suite_version,
        )
        _add_policy_expectation(
            suite,
            "expect_column_values_to_be_between",
            kwargs={"column": "total_amount", "min_value": 0.0},
            severity="critical",
            domain="financial_validity",
            rationale="negative total_amount indicates invalid fare math for clean-layer analytics.",
            suite_version=suite_version,
        )
        _add_policy_expectation(
            suite,
            "expect_column_pair_values_A_to_be_greater_than_B",
            kwargs={
                "column_A": "dropoff_ts",
                "column_B": "pickup_ts",
                "or_equal": True,
                "ignore_row_if": "either_value_is_missing",
                "mostly": mostly,
            },
            severity="critical",
            domain="temporal_integrity",
            rationale=(
                "dropoff_ts must be >= pickup_ts. "
                "A small tolerance (GE_MOSTLY) preserves pipeline stability for rare source edge cases."
            ),
            suite_version=suite_version,
        )
    elif policy == "warning":
        _add_policy_expectation(
            suite,
            "expect_column_values_to_be_between",
            kwargs={"column": "trip_distance", "min_value": 0.0},
            severity="warning",
            domain="realism",
            rationale="negative trip_distance is suspicious and should be monitored for source anomalies.",
            suite_version=suite_version,
        )
        _add_policy_expectation(
            suite,
            "expect_column_values_to_be_between",
            kwargs={
                "column": "passenger_count",
                "min_value": 0,
                "max_value": 8,
                "mostly": mostly,
            },
            severity="warning",
            domain="realism",
            rationale="out-of-band passenger_count values are unusual but can occur in dirty source extracts.",
            suite_version=suite_version,
        )
        _add_policy_expectation(
            suite,
            "expect_column_values_to_be_in_set",
            kwargs={
                "column": "payment_type",
                "value_set": [1, 2, 3, 4, 5, 6],
                "mostly": payment_mostly,
            },
            severity="warning",
            domain="domain_monitoring",
            rationale=(
                "payment_type should follow TLC codes {1..6}. "
                "Unknown-coded 0 values are tracked as warning-level anomalies."
            ),
            suite_version=suite_version,
        )
    else:
        raise ValueError(f"Unsupported GE policy: {policy}")

    return suite


def _extract_expectation_counts(payload: Dict[str, Any]) -> Dict[str, int]:
    evaluated = 0
    failed = 0

    validation_results = payload.get("validation_results")
    if isinstance(validation_results, list):
        for validation in validation_results:
            if not isinstance(validation, dict):
                continue

            stats = validation.get("statistics")
            if isinstance(stats, dict):
                eval_count = int(stats.get("evaluated_expectations", 0) or 0)
                success_count = int(stats.get("successful_expectations", 0) or 0)
                failed_count = stats.get("unsuccessful_expectations")
                if failed_count is None:
                    failed_count = max(eval_count - success_count, 0)
                evaluated += eval_count
                failed += int(failed_count or 0)
                continue

            expectations = validation.get("expectations")
            if isinstance(expectations, list):
                evaluated += len(expectations)
                failed += sum(1 for item in expectations if isinstance(item, dict) and not bool(item.get("success", False)))

    if evaluated == 0:
        stats = payload.get("statistics")
        if isinstance(stats, dict):
            eval_count = stats.get("evaluated_expectations")
            success_count = stats.get("successful_expectations")
            failed_count = stats.get("unsuccessful_expectations")
            if eval_count is not None:
                evaluated = int(eval_count or 0)
            if failed_count is not None:
                failed = int(failed_count or 0)
            elif evaluated and success_count is not None:
                failed = max(evaluated - int(success_count or 0), 0)

    return {"evaluated": evaluated, "failed": failed}


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


def _run_policy_checkpoint(
    context: gx.DataContext,
    batch_definition: Any,
    *,
    policy: str,
    suite_name: str,
    suite_version: str,
    validation_def_name: str,
    checkpoint_name: str,
    mostly: float,
    payment_mostly: float,
) -> Dict[str, Any]:
    suite = _build_suite(
        context,
        suite_name=suite_name,
        policy=policy,
        suite_version=suite_version,
        mostly=mostly,
        payment_mostly=payment_mostly,
    )
    validation_definition = _get_or_add_validation_definition(context, validation_def_name, batch_definition, suite)
    checkpoint = _get_or_add_checkpoint(context, checkpoint_name, validation_definition)
    result = checkpoint.run()
    described = result.describe() if hasattr(result, "describe") else result
    return _safe_json_payload(described)


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


def main() -> None:
    fail_on_error = _env_flag("GE_FAIL_ON_ERROR", True)
    fail_on_warning = _env_flag("GE_FAIL_ON_WARNING", False)
    mostly = _env_float("GE_MOSTLY", 0.999)
    payment_mostly = _env_float("GE_PAYMENT_TYPE_MOSTLY", 0.95)

    # Keep GE_SUITE_NAME as a legacy base prefix, but always emit versioned policy suites.
    suite_prefix = os.getenv("GE_SUITE_NAME", "clean_yellow_trips")
    critical_version = _normalize_suite_version(os.getenv("GE_SUITE_VERSION_CRITICAL", "v1"))
    warning_version = _normalize_suite_version(os.getenv("GE_SUITE_VERSION_WARNING", "v1"))

    critical_suite_name = os.getenv("GE_SUITE_NAME_CRITICAL", f"{suite_prefix}__critical__{critical_version}")
    warning_suite_name = os.getenv("GE_SUITE_NAME_WARNING", f"{suite_prefix}__warning__{warning_version}")

    checkpoint_prefix = os.getenv("GE_CHECKPOINT_NAME", "cp_clean_yellow_trips")
    validation_prefix = os.getenv("GE_VALIDATION_DEF_NAME", "vd_clean_yellow_trips_whole_table")

    critical_checkpoint_name = os.getenv(
        "GE_CHECKPOINT_NAME_CRITICAL", f"{checkpoint_prefix}__critical__{critical_version}"
    )
    warning_checkpoint_name = os.getenv("GE_CHECKPOINT_NAME_WARNING", f"{checkpoint_prefix}__warning__{warning_version}")
    critical_validation_def_name = os.getenv(
        "GE_VALIDATION_DEF_NAME_CRITICAL", f"{validation_prefix}__critical__{critical_version}"
    )
    warning_validation_def_name = os.getenv(
        "GE_VALIDATION_DEF_NAME_WARNING", f"{validation_prefix}__warning__{warning_version}"
    )

    data_source_name = os.getenv("GE_DATASOURCE_NAME", "nyc_postgres")
    asset_table_name = os.getenv("GE_TABLE_NAME", "clean_yellow_trips")
    asset_schema_name = os.getenv("GE_TABLE_SCHEMA", "clean")
    asset_name = os.getenv("GE_ASSET_NAME", asset_table_name)
    batch_definition_name = os.getenv("GE_BATCH_DEFINITION_NAME", "whole_table")

    repo_docs_dir = Path("docs") / "ge"
    repo_docs_dir.mkdir(parents=True, exist_ok=True)

    out_dir = Path("data") / "reports" / "ge"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 1) Context
    context = gx.get_context()

    # 2) Datasource / Asset / Batch
    conn_str = _pg_connection_string()
    data_source = _get_or_add_datasource(context, data_source_name, conn_str)

    data_asset = _get_or_add_table_asset(
        data_source,
        asset_name=asset_name,
        table_name=asset_table_name,
        schema_name=asset_schema_name,
    )

    batch_definition = _get_or_add_batch_definition_whole_table(data_asset, batch_definition_name)

    # 3) Run both policy suites in one checkpoint invocation script.
    critical_payload = _run_policy_checkpoint(
        context,
        batch_definition,
        policy="critical",
        suite_name=critical_suite_name,
        suite_version=critical_version,
        validation_def_name=critical_validation_def_name,
        checkpoint_name=critical_checkpoint_name,
        mostly=mostly,
        payment_mostly=payment_mostly,
    )
    warning_payload = _run_policy_checkpoint(
        context,
        batch_definition,
        policy="warning",
        suite_name=warning_suite_name,
        suite_version=warning_version,
        validation_def_name=warning_validation_def_name,
        checkpoint_name=warning_checkpoint_name,
        mostly=mostly,
        payment_mostly=payment_mostly,
    )

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

    # 8) Compute auditable summaries.
    critical_success = bool(critical_payload.get("success", True))
    warning_success = bool(warning_payload.get("success", True))
    critical_counts = _extract_expectation_counts(critical_payload)
    warning_counts = _extract_expectation_counts(warning_payload)
    should_fail = (fail_on_error and not critical_success) or (fail_on_warning and not warning_success)

    combined_payload: Dict[str, Any] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "success": critical_success and warning_success,
        "table": {"schema": asset_schema_name, "name": asset_table_name},
        "fail_policy": {
            "GE_FAIL_ON_ERROR": int(fail_on_error),
            "GE_FAIL_ON_WARNING": int(fail_on_warning),
            "exit_nonzero": int(should_fail),
        },
        "suites": {
            "critical": {
                "name": critical_suite_name,
                "version": critical_version,
                "checkpoint_name": critical_checkpoint_name,
                "validation_definition_name": critical_validation_def_name,
                "success": critical_success,
                "evaluated_expectations": critical_counts["evaluated"],
                "failed_expectations": critical_counts["failed"],
            },
            "warning": {
                "name": warning_suite_name,
                "version": warning_version,
                "checkpoint_name": warning_checkpoint_name,
                "validation_definition_name": warning_validation_def_name,
                "success": warning_success,
                "evaluated_expectations": warning_counts["evaluated"],
                "failed_expectations": warning_counts["failed"],
            },
        },
        "results": {
            "critical": critical_payload,
            "warning": warning_payload,
        },
    }

    # 9) Save artifacts (combined + per-suite).
    out_path_combined = out_dir / f"checkpoint_result_{stamp}.json"
    out_path_critical = out_dir / f"checkpoint_result_{stamp}_critical.json"
    out_path_warning = out_dir / f"checkpoint_result_{stamp}_warning.json"

    _write_json(out_path_combined, combined_payload)
    _write_json(out_path_critical, critical_payload)
    _write_json(out_path_warning, warning_payload)
    print(f"[ge] wrote: {out_path_combined}")
    print(f"[ge] wrote: {out_path_critical}")
    print(f"[ge] wrote: {out_path_warning}")

    # 10) Console summary
    print("[ge] summary:")
    print(
        f"[ge] critical: {'PASSED' if critical_success else 'FAILED'} "
        f"(failed_expectations={critical_counts['failed']}/{critical_counts['evaluated']}) "
        f"suite={critical_suite_name}"
    )
    print(
        f"[ge] warning: {'PASSED' if warning_success else 'FAILED'} "
        f"(failed_expectations={warning_counts['failed']}/{warning_counts['evaluated']}) "
        f"suite={warning_suite_name}"
    )
    print(
        f"[ge] fail policy: GE_FAIL_ON_ERROR={int(fail_on_error)} "
        f"GE_FAIL_ON_WARNING={int(fail_on_warning)} exit_nonzero={int(should_fail)}"
    )
    print(json.dumps(combined_payload, indent=2, ensure_ascii=False, default=str))

    # 11) Fail pipeline if desired by policy.
    if should_fail:
        reasons = []
        if fail_on_error and not critical_success:
            reasons.append("critical suite failed with GE_FAIL_ON_ERROR=1")
        if fail_on_warning and not warning_success:
            reasons.append("warning suite failed with GE_FAIL_ON_WARNING=1")
        detail = "; ".join(reasons) if reasons else "GE policy requested failure"
        print(f"[ge] validation FAILED ({detail}). Exiting with code 1.")
        sys.exit(1)


if __name__ == "__main__":
    main()
