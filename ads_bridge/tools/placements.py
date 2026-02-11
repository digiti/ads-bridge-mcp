import asyncio
import json
from typing import Any

from .. import mcp
from ..client import call_google_tool, call_meta_tool
from ..normalize import compute_derived_metrics, meta_spend_to_micros, micros_to_display


def _empty_metrics() -> dict[str, Any]:
    return {
        "impressions": 0,
        "clicks": 0,
        "spend_micros": 0,
        "conversions": 0.0,
        "conversion_value": 0.0,
    }


def _extract_meta_conversions(item: dict[str, Any]) -> float:
    conversions = 0.0
    for action in item.get("actions", []):
        if action.get("action_type") in (
            "purchase",
            "lead",
            "complete_registration",
            "omni_purchase",
        ):
            conversions += float(action.get("value", 0) or 0)
    return conversions


def _extract_meta_conversion_value(item: dict[str, Any]) -> float:
    total = 0.0
    for action_value in item.get("action_values", []):
        if action_value.get("action_type") in (
            "purchase",
            "omni_purchase",
        ):
            total += float(action_value.get("value", 0) or 0)
    return total


def _finalize_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    impressions = int(metrics.get("impressions", 0))
    clicks = int(metrics.get("clicks", 0))
    spend_micros = int(metrics.get("spend_micros", 0))
    conversions = float(metrics.get("conversions", 0))
    conversion_value = float(metrics.get("conversion_value", 0))
    derived = compute_derived_metrics(impressions, clicks, spend_micros, conversions)
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
async def compare_placements(
    meta_account_ids: list[str],
    google_account_ids: list[str],
    date_start: str,
    date_end: str,
    google_login_customer_id: str | None = None,
) -> str:
    errors: list[dict[str, Any]] = []
    meta_raw: dict[str, Any] = {"accounts": {}}
    google_raw: dict[str, Any] = {"accounts": {}}

    meta_by_placement: dict[str, dict[str, Any]] = {}
    google_by_channel: dict[str, dict[str, Any]] = {}

    meta_tasks = [
        call_meta_tool(
            "get_insights",
            {
                "account_id": account_id,
                "time_range": {"since": date_start, "until": date_end},
                "level": "account",
                "breakdown": "publisher_platform",
            },
        )
        for account_id in meta_account_ids
    ]

    google_fields = [
        "customer.id",
        "customer.descriptive_name",
        "campaign.id",
        "campaign.name",
        "campaign.advertising_channel_type",
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

        for item in result.get("data", []):
            placement = str(item.get("publisher_platform", "") or "unknown").lower()
            bucket = meta_by_placement.setdefault(placement, _empty_metrics())
            bucket["impressions"] += int(item.get("impressions", 0) or 0)
            bucket["clicks"] += int(item.get("clicks", 0) or 0)
            bucket["spend_micros"] += meta_spend_to_micros(item.get("spend", 0))
            bucket["conversions"] += _extract_meta_conversions(item)
            bucket["conversion_value"] += _extract_meta_conversion_value(item)

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

        for item in result.get("data", []):
            channel = str(item.get("campaign.advertising_channel_type", "") or "unknown").lower()
            bucket = google_by_channel.setdefault(channel, _empty_metrics())
            bucket["impressions"] += int(item.get("metrics.impressions", 0) or 0)
            bucket["clicks"] += int(item.get("metrics.clicks", 0) or 0)
            bucket["spend_micros"] += int(item.get("metrics.cost_micros", 0) or 0)
            bucket["conversions"] += float(item.get("metrics.conversions", 0) or 0)
            bucket["conversion_value"] += float(item.get("metrics.conversions_value", 0) or 0)

    meta_placements = [
        {"placement": placement, **_finalize_metrics(metrics)}
        for placement, metrics in sorted(meta_by_placement.items(), key=lambda pair: pair[0])
    ]
    google_channels = [
        {"channel": channel, **_finalize_metrics(metrics)}
        for channel, metrics in sorted(google_by_channel.items(), key=lambda pair: pair[0])
    ]

    meta_totals = _finalize_metrics(
        {
            "impressions": sum(int(row["impressions"]) for row in meta_placements),
            "clicks": sum(int(row["clicks"]) for row in meta_placements),
            "spend_micros": sum(int(row["spend_micros"]) for row in meta_placements),
            "conversions": sum(float(row["conversions"]) for row in meta_placements),
            "conversion_value": sum(float(row["conversion_value"]) for row in meta_placements),
        }
    )
    google_totals = _finalize_metrics(
        {
            "impressions": sum(int(row["impressions"]) for row in google_channels),
            "clicks": sum(int(row["clicks"]) for row in google_channels),
            "spend_micros": sum(int(row["spend_micros"]) for row in google_channels),
            "conversions": sum(float(row["conversions"]) for row in google_channels),
            "conversion_value": sum(float(row["conversion_value"]) for row in google_channels),
        }
    )

    has_data = bool(meta_placements or google_channels)
    status = "ok" if not errors else ("partial" if has_data else "error")
    result: dict[str, Any] = {
        "status": status,
        "date_start": date_start,
        "date_end": date_end,
        "meta_placements": meta_placements,
        "google_channels": google_channels,
        "meta_totals": meta_totals,
        "google_totals": google_totals,
        "platform_results": {"meta": meta_raw, "google": google_raw},
    }
    if errors:
        result["errors"] = errors

    return json.dumps(result, indent=2)
