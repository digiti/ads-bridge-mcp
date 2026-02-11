import asyncio
import json
from typing import Any

from .. import mcp
from ..client import call_google_tool, call_meta_tool
from ..normalize import compute_derived_metrics, micros_to_display, normalize_google_insights, normalize_meta_insights


def _top_campaigns(rows: list[dict[str, Any]], limit: int = 3) -> list[dict[str, Any]]:
    ordered = sorted(rows, key=lambda row: int(row.get("spend_micros", 0)), reverse=True)
    return [
        {
            "platform": row.get("platform", ""),
            "campaign_id": row.get("campaign_id", ""),
            "campaign_name": row.get("campaign_name", ""),
            "account_id": row.get("account_id", ""),
            "account_name": row.get("account_name", ""),
            "spend_micros": int(row.get("spend_micros", 0)),
            "spend": micros_to_display(int(row.get("spend_micros", 0))),
            "impressions": int(row.get("impressions", 0)),
            "clicks": int(row.get("clicks", 0)),
            "conversions": float(row.get("conversions", 0)),
        }
        for row in ordered[:limit]
    ]


@mcp.tool()
async def get_cross_platform_summary(
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
                "level": "campaign",
            },
        )
        for account_id in meta_account_ids
    ]

    google_fields = [
        "customer.id",
        "customer.descriptive_name",
        "campaign.id",
        "campaign.name",
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
                "resource": "campaign",
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

    all_rows = meta_rows + google_rows

    total_spend_micros = sum(int(row.get("spend_micros", 0)) for row in all_rows)
    total_impressions = sum(int(row.get("impressions", 0)) for row in all_rows)
    total_clicks = sum(int(row.get("clicks", 0)) for row in all_rows)
    total_conversions = sum(float(row.get("conversions", 0)) for row in all_rows)
    total_conversion_value = sum(float(row.get("conversion_value", 0)) for row in all_rows)

    meta_spend = sum(int(row.get("spend_micros", 0)) for row in meta_rows)
    google_spend = sum(int(row.get("spend_micros", 0)) for row in google_rows)

    derived = compute_derived_metrics(total_impressions, total_clicks, total_spend_micros, total_conversions)
    spend_split = {
        "meta_pct": round((meta_spend / total_spend_micros) * 100, 2) if total_spend_micros else 0.0,
        "google_pct": round((google_spend / total_spend_micros) * 100, 2) if total_spend_micros else 0.0,
    }

    result: dict[str, Any] = {
        "status": "ok" if not errors else ("partial" if all_rows else "error"),
        "date_start": date_start,
        "date_end": date_end,
        "totals": {
            "spend_micros": total_spend_micros,
            "spend": micros_to_display(total_spend_micros),
            "impressions": total_impressions,
            "clicks": total_clicks,
            "conversions": round(total_conversions, 2),
            "conversion_value": round(total_conversion_value, 2),
            **derived,
        },
        "platform_split": {
            "meta": {"spend_micros": meta_spend, "spend": micros_to_display(meta_spend), "pct": spend_split["meta_pct"]},
            "google": {
                "spend_micros": google_spend,
                "spend": micros_to_display(google_spend),
                "pct": spend_split["google_pct"],
            },
        },
        "top_campaigns": {
            "meta": _top_campaigns(meta_rows, 3),
            "google": _top_campaigns(google_rows, 3),
        },
        "platform_results": {"meta": meta_raw, "google": google_raw},
    }
    if errors:
        result["errors"] = errors

    return json.dumps(result, indent=2)
