import asyncio
import json
from typing import Any

from .. import mcp
from ..client import call_google_tool, call_meta_tool
from ..normalize import compute_derived_metrics, meta_spend_to_micros, micros_to_display


_META_CONVERSION_ACTION_TYPES = {
    "purchase",
    "lead",
    "complete_registration",
    "omni_purchase",
}

_GOOGLE_DEVICE_MAP = {
    "MOBILE": "mobile",
    "DESKTOP": "desktop",
    "TABLET": "tablet",
    "CONNECTED_TV": "connected_tv",
    "OTHER": "other",
}


def _parse_meta_conversions(item: dict[str, Any]) -> float:
    conversions = 0.0
    for action in item.get("actions", []):
        if action.get("action_type") in _META_CONVERSION_ACTION_TYPES:
            conversions += float(action.get("value", 0))
    return conversions


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


def _device_sort_key(device: str) -> tuple[int, str]:
    order = {
        "mobile": 1,
        "desktop": 2,
        "tablet": 3,
        "connected_tv": 4,
        "other": 5,
        "unknown": 99,
    }
    return (order.get(device, 98), device)


@mcp.tool()
async def compare_device_performance(
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
                "breakdown": "device_platform",
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
        "segments.device",
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
            device = str(item.get("device_platform", "unknown") or "unknown").lower()
            impressions = int(item.get("impressions", 0))
            clicks = int(item.get("clicks", 0))
            spend_micros = meta_spend_to_micros(item.get("spend", "0"))
            conversions = _parse_meta_conversions(item)
            meta_rows.append(
                {
                    "device": device,
                    "impressions": impressions,
                    "clicks": clicks,
                    "spend_micros": spend_micros,
                    "conversions": conversions,
                }
            )

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
            raw_device = str(item.get("segments.device", "OTHER") or "")
            device = _GOOGLE_DEVICE_MAP.get(raw_device, "other")
            impressions = int(item.get("metrics.impressions", 0))
            clicks = int(item.get("metrics.clicks", 0))
            spend_micros = int(item.get("metrics.cost_micros", 0))
            conversions = float(item.get("metrics.conversions", 0))
            google_rows.append(
                {
                    "device": device,
                    "impressions": impressions,
                    "clicks": clicks,
                    "spend_micros": spend_micros,
                    "conversions": conversions,
                }
            )

    meta_by_device: dict[str, list[dict[str, Any]]] = {}
    google_by_device: dict[str, list[dict[str, Any]]] = {}
    for row in meta_rows:
        meta_by_device.setdefault(str(row["device"]), []).append(row)
    for row in google_rows:
        google_by_device.setdefault(str(row["device"]), []).append(row)

    device_keys = sorted(
        set(meta_by_device.keys()) | set(google_by_device.keys()),
        key=_device_sort_key,
    )

    segments: list[dict[str, Any]] = []
    for device in device_keys:
        meta_device_rows = meta_by_device.get(device, [])
        google_device_rows = google_by_device.get(device, [])
        combined_rows = meta_device_rows + google_device_rows
        segments.append(
            {
                "segment": device,
                "meta": _aggregate_rows(meta_device_rows),
                "google": _aggregate_rows(google_device_rows),
                "combined": _aggregate_rows(combined_rows),
            }
        )

    result: dict[str, Any] = {
        "status": "ok" if not errors else ("partial" if segments else "error"),
        "dimension": "device",
        "date_start": date_start,
        "date_end": date_end,
        "segments": segments,
        "platform_results": {"meta": meta_raw, "google": google_raw},
    }
    if errors:
        result["errors"] = errors

    return json.dumps(result, indent=2)
