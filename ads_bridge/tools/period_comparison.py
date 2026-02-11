import asyncio
import json
from typing import Any

from .. import mcp
from ..client import call_google_tool, call_meta_tool
from ..normalize import (
    attach_diagnostics,
    compute_derived_metrics,
    micros_to_display,
    normalize_google_insights,
    normalize_meta_insights,
    safe_divide,
)


def _aggregate_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    impressions = sum(int(row.get("impressions", 0)) for row in rows)
    clicks = sum(int(row.get("clicks", 0)) for row in rows)
    spend_micros = sum(int(row.get("spend_micros", 0)) for row in rows)
    conversions = sum(float(row.get("conversions", 0)) for row in rows)
    derived = compute_derived_metrics(impressions, clicks, spend_micros, conversions)
    return {
        "impressions": impressions,
        "clicks": clicks,
        "spend_micros": spend_micros,
        "spend": micros_to_display(spend_micros),
        "conversions": round(conversions, 2),
        "ctr": float(derived["ctr"]),
        "cpc_micros": int(derived["cpc_micros"]),
        "cpm_micros": int(derived["cpm_micros"]),
    }


def _build_change(current: dict[str, Any], previous: dict[str, Any]) -> dict[str, Any]:
    metrics = ["impressions", "clicks", "spend_micros", "conversions", "ctr", "cpc_micros"]
    changes: dict[str, Any] = {}
    for metric in metrics:
        current_value = float(current.get(metric, 0))
        previous_value = float(previous.get(metric, 0))
        absolute = current_value - previous_value
        changes[metric] = {
            "absolute": round(absolute, 2),
            "pct": round(safe_divide(absolute, previous_value) * 100, 2),
        }
    return changes


async def _fetch_period(
    meta_account_ids: list[str],
    google_account_ids: list[str],
    date_start: str,
    date_end: str,
    google_login_customer_id: str | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    errors: list[dict[str, Any]] = []
    meta_rows: list[dict[str, Any]] = []
    google_rows: list[dict[str, Any]] = []
    meta_raw: dict[str, Any] = {"accounts": {}}
    google_raw: dict[str, Any] = {"accounts": {}}

    meta_tasks = [
        call_meta_tool(
            "get_insights",
            {
                "account_id": account_id,
                "time_range": {"since": date_start, "until": date_end},
                "level": "account",
            },
        )
        for account_id in meta_account_ids
    ]

    google_fields = [
        "customer.id",
        "customer.descriptive_name",
        "metrics.impressions",
        "metrics.clicks",
        "metrics.cost_micros",
        "metrics.conversions",
        "metrics.conversions_value",
        "segments.date",
    ]
    google_tasks = [
        call_google_tool(
            "search_ads",
            {
                "customer_id": account_id,
                "resource": "customer",
                "fields": google_fields,
                "conditions": [f"segments.date BETWEEN '{date_start}' AND '{date_end}'"],
                **({"login_customer_id": google_login_customer_id} if google_login_customer_id else {}),
            },
        )
        for account_id in google_account_ids
    ]

    meta_results = await asyncio.gather(*meta_tasks, return_exceptions=True)
    google_results = await asyncio.gather(*google_tasks, return_exceptions=True)

    for idx, account_id in enumerate(meta_account_ids):
        raw_result = meta_results[idx]
        if isinstance(raw_result, BaseException):
            message = str(raw_result)
            errors.append({"platform": "meta", "account_id": account_id, "error": message})
            meta_raw["accounts"][account_id] = {"error": message}
            continue
        if not isinstance(raw_result, dict):
            message = f"Unexpected Meta response type: {type(raw_result).__name__}"
            errors.append({"platform": "meta", "account_id": account_id, "error": message})
            meta_raw["accounts"][account_id] = {"error": message}
            continue
        result = raw_result
        meta_raw["accounts"][account_id] = result
        if "error" in result:
            errors.append({"platform": "meta", "account_id": account_id, "error": str(result["error"])})
            continue
        meta_rows.extend(normalize_meta_insights(result))

    for idx, account_id in enumerate(google_account_ids):
        raw_result = google_results[idx]
        if isinstance(raw_result, BaseException):
            message = str(raw_result)
            errors.append({"platform": "google", "account_id": account_id, "error": message})
            google_raw["accounts"][account_id] = {"error": message}
            continue
        if not isinstance(raw_result, dict):
            message = f"Unexpected Google response type: {type(raw_result).__name__}"
            errors.append({"platform": "google", "account_id": account_id, "error": message})
            google_raw["accounts"][account_id] = {"error": message}
            continue
        result = raw_result
        google_raw["accounts"][account_id] = result
        if "error" in result:
            errors.append({"platform": "google", "account_id": account_id, "error": str(result["error"])})
            continue
        google_rows.extend(normalize_google_insights(result))

    return meta_rows, google_rows, meta_raw, google_raw, errors


@mcp.tool()
async def get_period_comparison(
    meta_account_ids: list[str],
    google_account_ids: list[str],
    date_start: str,
    date_end: str,
    compare_date_start: str,
    compare_date_end: str,
    google_login_customer_id: str | None = None,
    include_raw: bool = False,
) -> str:
    """Compare current and previous period performance across Meta and Google Ads.

    Use when: You need period-over-period deltas for key metrics across platforms,
    such as week-over-week or month-over-month shifts in spend and efficiency.

    Args:
        meta_account_ids: Meta ad account IDs to include in both periods.
        google_account_ids: Google Ads customer IDs to include in both periods.
        date_start: Inclusive start date for the current period in YYYY-MM-DD format.
        date_end: Inclusive end date for the current period in YYYY-MM-DD format.
        compare_date_start: Inclusive start date for the comparison period in YYYY-MM-DD format.
        compare_date_end: Inclusive end date for the comparison period in YYYY-MM-DD format.
        google_login_customer_id: Optional manager account ID for Google Ads API access.
    """
    (
        current_meta_rows,
        current_google_rows,
        current_meta_raw,
        current_google_raw,
        current_errors,
    ) = await _fetch_period(meta_account_ids, google_account_ids, date_start, date_end, google_login_customer_id)

    (
        previous_meta_rows,
        previous_google_rows,
        previous_meta_raw,
        previous_google_raw,
        previous_errors,
    ) = await _fetch_period(
        meta_account_ids,
        google_account_ids,
        compare_date_start,
        compare_date_end,
        google_login_customer_id,
    )

    errors = current_errors + previous_errors

    current_meta = _aggregate_rows(current_meta_rows)
    previous_meta = _aggregate_rows(previous_meta_rows)
    current_google = _aggregate_rows(current_google_rows)
    previous_google = _aggregate_rows(previous_google_rows)
    current_combined = _aggregate_rows(current_meta_rows + current_google_rows)
    previous_combined = _aggregate_rows(previous_meta_rows + previous_google_rows)

    comparison = {
        "meta": {
            "current": current_meta,
            "previous": previous_meta,
            "change": _build_change(current_meta, previous_meta),
        },
        "google": {
            "current": current_google,
            "previous": previous_google,
            "change": _build_change(current_google, previous_google),
        },
        "combined": {
            "current": current_combined,
            "previous": previous_combined,
            "change": _build_change(current_combined, previous_combined),
        },
    }

    has_data = bool(
        current_meta_rows
        or current_google_rows
        or previous_meta_rows
        or previous_google_rows
    )
    result: dict[str, Any] = {
        "status": "ok" if not errors else ("partial" if has_data else "error"),
        "current_period": {"date_start": date_start, "date_end": date_end},
        "previous_period": {"date_start": compare_date_start, "date_end": compare_date_end},
        "comparison": comparison,
    }
    if errors:
        result["errors"] = errors

    meta_raw = {
        "accounts": {
            **current_meta_raw.get("accounts", {}),
            **{f"previous:{k}": v for k, v in previous_meta_raw.get("accounts", {}).items()},
        }
    }
    google_raw = {
        "accounts": {
            **current_google_raw.get("accounts", {}),
            **{f"previous:{k}": v for k, v in previous_google_raw.get("accounts", {}).items()},
        }
    }
    attach_diagnostics(result, meta_raw, google_raw, include_raw)

    return json.dumps(result, indent=2)
