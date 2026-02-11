import asyncio
import json
from typing import Any

from .. import mcp
from ..client import call_google_tool, call_meta_tool
from ..normalize import compute_derived_metrics, meta_spend_to_micros, micros_to_display


COUNTRY_ID_TO_ISO = {
    "2056": "BE",
    "2528": "NL",
    "2250": "FR",
    "2276": "DE",
    "2826": "GB",
    "2840": "US",
    "2724": "ES",
    "2380": "IT",
    "2620": "PT",
    "2040": "AT",
    "2756": "CH",
    "2208": "DK",
    "2752": "SE",
    "2578": "NO",
    "2246": "FI",
    "2372": "IE",
    "2616": "PL",
    "2203": "CZ",
}


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


def _country_from_google_row(item: dict[str, Any]) -> str:
    raw_country = str(item.get("segments.geo_target_country", "") or item.get("geographic_view.country_criterion_id", "")).strip()
    if not raw_country:
        return "unknown"

    if raw_country.startswith("geoTargetConstants/"):
        country_id = raw_country.split("/")[-1]
        return COUNTRY_ID_TO_ISO.get(country_id, raw_country)

    if raw_country.isdigit():
        return COUNTRY_ID_TO_ISO.get(raw_country, raw_country)

    return raw_country.upper()


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
async def compare_geo_performance(
    meta_account_ids: list[str],
    google_account_ids: list[str],
    date_start: str,
    date_end: str,
    google_login_customer_id: str | None = None,
) -> str:
    errors: list[dict[str, Any]] = []
    meta_raw: dict[str, Any] = {"accounts": {}}
    google_raw: dict[str, Any] = {"accounts": {}}

    meta_by_country: dict[str, dict[str, Any]] = {}
    google_by_country: dict[str, dict[str, Any]] = {}

    meta_tasks = [
        call_meta_tool(
            "get_insights",
            {
                "account_id": account_id,
                "time_range": {"since": date_start, "until": date_end},
                "level": "account",
                "breakdown": "country",
            },
        )
        for account_id in meta_account_ids
    ]

    google_fields = [
        "geographic_view.country_criterion_id",
        "geographic_view.resource_name",
        "customer.id",
        "customer.descriptive_name",
        "metrics.impressions",
        "metrics.clicks",
        "metrics.cost_micros",
        "metrics.conversions",
        "metrics.conversions_value",
        "segments.date",
        "segments.geo_target_country",
    ]
    google_tasks = [
        call_google_tool(
            "search_ads",
            {
                "customer_id": account_id,
                "resource": "geographic_view",
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
            country = str(item.get("country", "") or "unknown").upper()
            bucket = meta_by_country.setdefault(country, _empty_metrics())
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
            country = _country_from_google_row(item)
            bucket = google_by_country.setdefault(country, _empty_metrics())
            bucket["impressions"] += int(item.get("metrics.impressions", 0) or 0)
            bucket["clicks"] += int(item.get("metrics.clicks", 0) or 0)
            bucket["spend_micros"] += int(item.get("metrics.cost_micros", 0) or 0)
            bucket["conversions"] += float(item.get("metrics.conversions", 0) or 0)
            bucket["conversion_value"] += float(item.get("metrics.conversions_value", 0) or 0)

    segment_keys = sorted(set(meta_by_country.keys()) | set(google_by_country.keys()))
    segments: list[dict[str, Any]] = []
    for segment in segment_keys:
        meta_metrics = _finalize_metrics(meta_by_country.get(segment, _empty_metrics()))
        google_metrics = _finalize_metrics(google_by_country.get(segment, _empty_metrics()))
        combined_raw = {
            "impressions": int(meta_metrics["impressions"]) + int(google_metrics["impressions"]),
            "clicks": int(meta_metrics["clicks"]) + int(google_metrics["clicks"]),
            "spend_micros": int(meta_metrics["spend_micros"]) + int(google_metrics["spend_micros"]),
            "conversions": float(meta_metrics["conversions"]) + float(google_metrics["conversions"]),
            "conversion_value": float(meta_metrics["conversion_value"]) + float(google_metrics["conversion_value"]),
        }
        segments.append(
            {
                "segment": segment,
                "meta": meta_metrics,
                "google": google_metrics,
                "combined": _finalize_metrics(combined_raw),
            }
        )

    status = "ok" if not errors else ("partial" if segments else "error")
    result: dict[str, Any] = {
        "status": status,
        "dimension": "country",
        "date_start": date_start,
        "date_end": date_end,
        "segments": segments,
        "platform_results": {"meta": meta_raw, "google": google_raw},
    }
    if errors:
        result["errors"] = errors

    return json.dumps(result, indent=2)
