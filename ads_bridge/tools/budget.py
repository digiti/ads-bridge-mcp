import asyncio
import json
from typing import Any

from .. import mcp
from ..client import call_google_tool, call_meta_tool
from ..normalize import micros_to_display, normalize_google_insights, normalize_meta_insights, safe_divide


def _platform_totals(rows: list[dict[str, Any]]) -> dict[str, float]:
    spend_micros = float(sum(int(row.get("spend_micros", 0)) for row in rows))
    conversion_value = float(sum(float(row.get("conversion_value", 0)) for row in rows))
    roas = safe_divide(conversion_value, spend_micros / 1_000_000) if spend_micros else 0.0
    return {
        "spend_micros": spend_micros,
        "conversion_value": conversion_value,
        "roas": roas,
    }


def _build_recommendation(meta_roas: float, google_roas: float) -> str:
    if meta_roas <= 0 and google_roas <= 0:
        return "ROAS data is limited; keep current allocation until conversion value tracking improves."
    if meta_roas > google_roas * 1.2:
        return "Meta shows materially stronger ROAS; consider reallocating incremental budget toward Meta campaigns."
    if google_roas > meta_roas * 1.2:
        return "Google shows materially stronger ROAS; consider reallocating incremental budget toward Google campaigns."
    return "ROAS is relatively balanced; maintain current split and optimize within each platform."


@mcp.tool()
async def get_budget_allocation(
    meta_account_ids: list[str],
    google_account_ids: list[str],
    date_start: str,
    date_end: str,
    google_login_customer_id: str | None = None,
) -> str:
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

    meta_totals = _platform_totals(meta_rows)
    google_totals = _platform_totals(google_rows)

    total_spend_micros = int(meta_totals["spend_micros"] + google_totals["spend_micros"])
    meta_spend_micros = int(meta_totals["spend_micros"])
    google_spend_micros = int(google_totals["spend_micros"])

    meta_pct = round((meta_spend_micros / total_spend_micros) * 100, 2) if total_spend_micros else 0.0
    google_pct = round((google_spend_micros / total_spend_micros) * 100, 2) if total_spend_micros else 0.0

    result: dict[str, Any] = {
        "status": "ok" if not errors else ("partial" if total_spend_micros > 0 else "error"),
        "date_start": date_start,
        "date_end": date_end,
        "spend_allocation": {
            "meta": {
                "spend_micros": meta_spend_micros,
                "spend": micros_to_display(meta_spend_micros),
                "pct": meta_pct,
                "roas": round(meta_totals["roas"], 4),
            },
            "google": {
                "spend_micros": google_spend_micros,
                "spend": micros_to_display(google_spend_micros),
                "pct": google_pct,
                "roas": round(google_totals["roas"], 4),
            },
            "total_spend_micros": total_spend_micros,
            "total_spend": micros_to_display(total_spend_micros),
        },
        "recommendation": _build_recommendation(meta_totals["roas"], google_totals["roas"]),
        "platform_results": {"meta": meta_raw, "google": google_raw},
    }
    if errors:
        result["errors"] = errors

    return json.dumps(result, indent=2)
