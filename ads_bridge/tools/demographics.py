import asyncio
import json
from typing import Any

from .. import mcp
from ..client import call_google_tool, call_meta_tool
from ..normalize import compute_derived_metrics, meta_spend_to_micros, micros_to_display, safe_divide


_META_CONVERSION_ACTION_TYPES = {
    "purchase",
    "lead",
    "complete_registration",
    "omni_purchase",
}

_GOOGLE_AGE_MAP = {
    "AGE_RANGE_18_24": "18-24",
    "AGE_RANGE_25_34": "25-34",
    "AGE_RANGE_35_44": "35-44",
    "AGE_RANGE_45_54": "45-54",
    "AGE_RANGE_55_64": "55-64",
    "AGE_RANGE_65_UP": "65+",
    "AGE_RANGE_UNDETERMINED": "unknown",
}

_GOOGLE_GENDER_MAP = {
    "MALE": "male",
    "FEMALE": "female",
    "UNDETERMINED": "unknown",
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


def _segment_sort_key(segment: str, dimension: str) -> tuple[int, str]:
    if dimension == "age":
        order = {
            "18-24": 1,
            "25-34": 2,
            "35-44": 3,
            "45-54": 4,
            "55-64": 5,
            "65+": 6,
            "unknown": 99,
        }
        return (order.get(segment, 98), segment)
    if dimension == "gender":
        order = {"male": 1, "female": 2, "unknown": 99}
        return (order.get(segment, 98), segment)
    return (98, segment)


@mcp.tool()
async def compare_demographics(
    meta_account_ids: list[str],
    google_account_ids: list[str],
    date_start: str,
    date_end: str,
    google_login_customer_id: str | None = None,
    dimension: str = "age",
) -> str:
    allowed_dimensions = {"age", "gender"}
    if dimension not in allowed_dimensions:
        return json.dumps(
            {
                "status": "error",
                "segments": [],
                "errors": [
                    {
                        "source": "validation",
                        "error": f"dimension must be one of {sorted(allowed_dimensions)}",
                    }
                ],
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
                "level": "account",
                "breakdown": dimension,
            },
        )
        for account_id in meta_account_ids
    ]

    if dimension == "age":
        google_resource = "age_range_view"
        google_fields = [
            "ad_group_criterion.age_range_type",
            "metrics.impressions",
            "metrics.clicks",
            "metrics.cost_micros",
            "metrics.conversions",
            "metrics.conversions_value",
            "customer.id",
            "customer.descriptive_name",
        ]
    else:
        google_resource = "gender_view"
        google_fields = [
            "ad_group_criterion.gender.type",
            "metrics.impressions",
            "metrics.clicks",
            "metrics.cost_micros",
            "metrics.conversions",
            "metrics.conversions_value",
            "customer.id",
            "customer.descriptive_name",
        ]

    google_tasks = [
        call_google_tool(
            "search_ads",
            {
                "customer_id": account_id,
                "resource": google_resource,
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
            raw_segment = str(item.get(dimension, "unknown") or "unknown")
            segment = raw_segment.lower() if dimension == "gender" else raw_segment
            impressions = int(item.get("impressions", 0))
            clicks = int(item.get("clicks", 0))
            spend_micros = meta_spend_to_micros(item.get("spend", "0"))
            conversions = _parse_meta_conversions(item)
            meta_rows.append(
                {
                    "segment": segment,
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
            if dimension == "age":
                raw_segment = str(item.get("ad_group_criterion.age_range_type", "AGE_RANGE_UNDETERMINED") or "")
                segment = _GOOGLE_AGE_MAP.get(raw_segment, "unknown")
            else:
                raw_segment = str(item.get("ad_group_criterion.gender.type", "UNDETERMINED") or "")
                segment = _GOOGLE_GENDER_MAP.get(raw_segment, "unknown")

            impressions = int(item.get("metrics.impressions", 0))
            clicks = int(item.get("metrics.clicks", 0))
            spend_micros = int(item.get("metrics.cost_micros", 0))
            conversions = float(item.get("metrics.conversions", 0))
            google_rows.append(
                {
                    "segment": segment,
                    "impressions": impressions,
                    "clicks": clicks,
                    "spend_micros": spend_micros,
                    "conversions": conversions,
                }
            )

    meta_by_segment: dict[str, list[dict[str, Any]]] = {}
    google_by_segment: dict[str, list[dict[str, Any]]] = {}
    for row in meta_rows:
        meta_by_segment.setdefault(str(row["segment"]), []).append(row)
    for row in google_rows:
        google_by_segment.setdefault(str(row["segment"]), []).append(row)

    segment_keys = sorted(
        set(meta_by_segment.keys()) | set(google_by_segment.keys()),
        key=lambda segment: _segment_sort_key(segment, dimension),
    )

    segments: list[dict[str, Any]] = []
    for segment in segment_keys:
        meta_segment_rows = meta_by_segment.get(segment, [])
        google_segment_rows = google_by_segment.get(segment, [])
        combined_rows = meta_segment_rows + google_segment_rows
        meta_totals = _aggregate_rows(meta_segment_rows)
        google_totals = _aggregate_rows(google_segment_rows)
        combined_totals = _aggregate_rows(combined_rows)
        combined_totals["share_pct"] = round(
            safe_divide(combined_totals["spend_micros"], sum(int(r.get("spend_micros", 0)) for r in meta_rows + google_rows))
            * 100,
            2,
        )
        segments.append(
            {
                "segment": segment,
                "meta": meta_totals,
                "google": google_totals,
                "combined": combined_totals,
            }
        )

    result: dict[str, Any] = {
        "status": "ok" if not errors else ("partial" if segments else "error"),
        "dimension": dimension,
        "date_start": date_start,
        "date_end": date_end,
        "segments": segments,
        "platform_results": {"meta": meta_raw, "google": google_raw},
    }
    if errors:
        result["errors"] = errors

    return json.dumps(result, indent=2)
