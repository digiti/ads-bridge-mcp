import asyncio
import json
from typing import Any

from .. import mcp
from ..client import call_google_tool, call_meta_tool
from ..normalize import micros_to_display, normalize_google_insights, normalize_meta_insights


@mcp.tool()
async def compare_top_campaigns(
    meta_account_ids: list[str],
    google_account_ids: list[str],
    date_start: str,
    date_end: str,
    sort_by: str = "spend",
    limit: int = 10,
    google_login_customer_id: str | None = None,
) -> str:
    valid_sort = {"spend", "impressions", "clicks", "conversions"}
    if sort_by not in valid_sort:
        return json.dumps(
            {
                "status": "error",
                "rows": [],
                "errors": [{"source": "validation", "error": f"sort_by must be one of {sorted(valid_sort)}"}],
            },
            indent=2,
        )

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

    metric_key = {
        "spend": "spend_micros",
        "impressions": "impressions",
        "clicks": "clicks",
        "conversions": "conversions",
    }[sort_by]

    all_rows = meta_rows + google_rows
    ranked = sorted(all_rows, key=lambda row: float(row.get(metric_key, 0)), reverse=True)[: max(limit, 0)]
    output_rows = [
        {
            "rank": index + 1,
            "platform": row.get("platform", ""),
            "account_id": row.get("account_id", ""),
            "account_name": row.get("account_name", ""),
            "campaign_id": row.get("campaign_id", ""),
            "campaign_name": row.get("campaign_name", ""),
            "spend_micros": int(row.get("spend_micros", 0)),
            "spend": micros_to_display(int(row.get("spend_micros", 0))),
            "impressions": int(row.get("impressions", 0)),
            "clicks": int(row.get("clicks", 0)),
            "conversions": float(row.get("conversions", 0)),
            "sort_metric": sort_by,
            "sort_value": row.get(metric_key, 0),
        }
        for index, row in enumerate(ranked)
    ]

    result: dict[str, Any] = {
        "status": "ok" if not errors else ("partial" if output_rows else "error"),
        "sort_by": sort_by,
        "limit": limit,
        "date_start": date_start,
        "date_end": date_end,
        "rows": output_rows,
        "platform_results": {"meta": meta_raw, "google": google_raw},
    }
    if errors:
        result["errors"] = errors

    return json.dumps(result, indent=2)
