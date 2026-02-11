import asyncio
import json
from typing import Any

from .. import mcp
from ..client import call_google_tool, call_meta_tool
from ..normalize import InvalidDateError, attach_diagnostics, compute_derived_metrics, micros_to_display, normalize_google_insights, normalize_meta_insights, validate_date


def _aggregate_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    impressions = sum(int(row.get("impressions", 0)) for row in rows)
    clicks = sum(int(row.get("clicks", 0)) for row in rows)
    spend_micros = sum(int(row.get("spend_micros", 0)) for row in rows)
    conversions = sum(float(row.get("conversions", 0)) for row in rows)
    conversion_value = sum(float(row.get("conversion_value", 0)) for row in rows)
    derived = compute_derived_metrics(impressions, clicks, spend_micros, conversions, conversion_value)
    return {
        "impressions": impressions,
        "clicks": clicks,
        "spend_micros": spend_micros,
        "spend": micros_to_display(spend_micros),
        "conversions": round(conversions, 2),
        "conversion_value": round(conversion_value, 2),
        **derived,
    }


@mcp.tool()
async def compare_daily_trends(
    meta_account_ids: list[str],
    google_account_ids: list[str],
    date_start: str,
    date_end: str,
    google_login_customer_id: str | None = None,
    include_raw: bool = False,
) -> str:
    """Compare daily account-level performance trends across Meta and Google Ads.

    Use when: You need a day-by-day cross-platform timeline to diagnose trend
    changes, pacing shifts, or performance divergence over a selected date range.

    Differs from compare_performance: compare_performance aggregates the full
    range into rollup rows; this tool returns one entry per day.
    Differs from get_period_comparison: period_comparison computes deltas between
    two date ranges; this tool shows the day-by-day shape within a single range.

    Args:
        meta_account_ids: Meta ad account IDs to include in daily trend aggregation.
        google_account_ids: Google Ads customer IDs to include in daily trend aggregation.
        date_start: Inclusive start date for the trend window in YYYY-MM-DD format.
        date_end: Inclusive end date for the trend window in YYYY-MM-DD format.
        google_login_customer_id: Optional manager account ID for Google Ads API access.
    """
    try:
        validate_date(date_start)
        validate_date(date_end)
    except InvalidDateError as exc:
        result = {"status": "error", "daily": [], "errors": [{"source": "validation", "error": str(exc)}]}
        attach_diagnostics(result)
        return json.dumps(result, indent=2)

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
                "time_increment": 1,
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

    dates = {
        str(row.get("date_start", ""))
        for row in meta_rows + google_rows
        if str(row.get("date_start", ""))
    }

    daily: list[dict[str, Any]] = []
    for date in sorted(dates):
        meta_date_rows = [row for row in meta_rows if str(row.get("date_start", "")) == date]
        google_date_rows = [row for row in google_rows if str(row.get("date_start", "")) == date]
        combined_rows = meta_date_rows + google_date_rows
        daily.append(
            {
                "date": date,
                "meta": _aggregate_metrics(meta_date_rows),
                "google": _aggregate_metrics(google_date_rows),
                "combined": _aggregate_metrics(combined_rows),
            }
        )

    result: dict[str, Any] = {
        "status": "ok" if not errors else ("partial" if daily else "error"),
        "date_start": date_start,
        "date_end": date_end,
        "daily": daily,
    }
    if errors:
        result["errors"] = errors

    attach_diagnostics(result, meta_raw, google_raw, include_raw)

    return json.dumps(result, indent=2)
