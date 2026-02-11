import asyncio
import json
from typing import Any

from .. import mcp
from ..client import call_google_tool, call_meta_tool
from ..normalize import InvalidDateError, attach_diagnostics, compute_derived_metrics, meta_spend_to_micros, micros_to_display, safe_divide, validate_date


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

_GOOGLE_DEVICE_MAP = {
    "MOBILE": "mobile",
    "DESKTOP": "desktop",
    "TABLET": "tablet",
    "CONNECTED_TV": "connected_tv",
    "OTHER": "other",
}

from ._geo_constants import COUNTRY_ID_TO_ISO


def _parse_meta_conversions(item: dict[str, Any]) -> float:
    conversions = 0.0
    for action in item.get("actions", []):
        if action.get("action_type") in _META_CONVERSION_ACTION_TYPES:
            conversions += float(action.get("value", 0) or 0)
    return conversions


def _extract_meta_conversion_value(item: dict[str, Any]) -> float:
    av_by_type = {
        av.get("action_type"): float(av.get("value", 0) or 0)
        for av in item.get("action_values", [])
        if isinstance(av, dict)
    }
    for action_type in ("purchase", "omni_purchase", "offsite_conversion.fb_pixel_purchase"):
        if action_type in av_by_type:
            return av_by_type[action_type]
    return 0.0


def _aggregate_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
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
        "ctr": float(derived["ctr"]),
        "cpc_micros": int(derived["cpc_micros"]),
        "cpm_micros": int(derived["cpm_micros"]),
    }


def _empty_metrics() -> dict[str, Any]:
    return {
        "impressions": 0,
        "clicks": 0,
        "spend_micros": 0,
        "conversions": 0.0,
        "conversion_value": 0.0,
    }


def _finalize_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    impressions = int(metrics.get("impressions", 0))
    clicks = int(metrics.get("clicks", 0))
    spend_micros = int(metrics.get("spend_micros", 0))
    conversions = float(metrics.get("conversions", 0))
    conversion_value = float(metrics.get("conversion_value", 0))
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


@mcp.tool()
async def compare_by_dimension(
    meta_account_ids: list[str],
    google_account_ids: list[str],
    date_start: str,
    date_end: str,
    dimension: str = "age",
    google_login_customer_id: str | None = None,
    include_raw: bool = False,
) -> str:
    """Compare Meta + Google performance side-by-side by a selected breakdown dimension.

    Parameters:
    - meta_account_ids: List of Meta ad account IDs to query.
    - google_account_ids: List of Google Ads customer IDs to query.
    - date_start: Inclusive start date in YYYY-MM-DD.
    - date_end: Inclusive end date in YYYY-MM-DD.
    - dimension: Breakdown to compare. Allowed values: age, gender, device, country, placement.
      - age: Meta age vs Google ad_group_criterion.age_range_type from age_range_view.
      - gender: Meta gender vs Google ad_group_criterion.gender.type from gender_view.
      - device: Meta device_platform vs Google segments.device from campaign.
      - country: Meta country vs Google geographic_view.country_criterion_id from geographic_view.
      - placement: Meta publisher_platform vs Google campaign.advertising_channel_type from campaign.
    - google_login_customer_id: Optional Google manager account ID for cross-account querying.

    Example usage scenarios:
    - Use `dimension="age"` or `dimension="gender"` to diagnose audience demographic skew across platforms.
    - Use `dimension="device"` to compare mobile/desktop efficiency and identify device-level spend shifts.
    - Use `dimension="country"` to evaluate geo performance including conversion value by market.
    - Use `dimension="placement"` to align Meta publisher platforms with Google channel-type performance.
    """
    try:
        validate_date(date_start)
        validate_date(date_end)
    except InvalidDateError as exc:
        result = {"status": "error", "dimension": dimension, "date_start": date_start, "date_end": date_end, "segments": [], "errors": [{"source": "validation", "error": str(exc)}]}
        attach_diagnostics(result)
        return json.dumps(result, indent=2)

    allowed_dimensions = {"age", "gender", "device", "country", "placement"}
    if dimension not in allowed_dimensions:
        result = {"status": "error", "dimension": dimension, "date_start": date_start, "date_end": date_end, "segments": [], "errors": [{"source": "validation", "error": f"dimension must be one of {sorted(allowed_dimensions)}"}]}
        attach_diagnostics(result)
        return json.dumps(result, indent=2)

    errors: list[dict[str, Any]] = []
    meta_raw: dict[str, Any] = {"accounts": {}}
    google_raw: dict[str, Any] = {"accounts": {}}

    meta_breakdown = {
        "age": "age",
        "gender": "gender",
        "device": "device_platform",
        "country": "country",
        "placement": "publisher_platform",
    }[dimension]

    google_resource = {
        "age": "age_range_view",
        "gender": "gender_view",
        "device": "campaign",
        "country": "geographic_view",
        "placement": "campaign",
    }[dimension]

    google_fields = {
        "age": [
            "ad_group_criterion.age_range_type",
            "metrics.impressions",
            "metrics.clicks",
            "metrics.cost_micros",
            "metrics.conversions",
            "metrics.conversions_value",
            "customer.id",
            "customer.descriptive_name",
        ],
        "gender": [
            "ad_group_criterion.gender.type",
            "metrics.impressions",
            "metrics.clicks",
            "metrics.cost_micros",
            "metrics.conversions",
            "metrics.conversions_value",
            "customer.id",
            "customer.descriptive_name",
        ],
        "device": [
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
        ],
        "country": [
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
        ],
        "placement": [
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
        ],
    }[dimension]

    meta_tasks = [
        call_meta_tool(
            "get_insights",
            {
                "account_id": account_id,
                "time_range": {"since": date_start, "until": date_end},
                "level": "account",
                "breakdown": meta_breakdown,
            },
        )
        for account_id in meta_account_ids
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

    if dimension in {"age", "gender", "device"}:
        meta_rows: list[dict[str, Any]] = []
        google_rows: list[dict[str, Any]] = []

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
                if dimension == "device":
                    segment = str(item.get("device_platform", "unknown") or "unknown").lower()
                else:
                    raw_segment = str(item.get(dimension, "unknown") or "unknown")
                    segment = raw_segment.lower() if dimension == "gender" else raw_segment
                meta_rows.append(
                    {
                        "segment": segment,
                        "impressions": int(item.get("impressions", 0)),
                        "clicks": int(item.get("clicks", 0)),
                        "spend_micros": meta_spend_to_micros(item.get("spend", "0")),
                        "conversions": _parse_meta_conversions(item),
                        "conversion_value": _extract_meta_conversion_value(item),
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
                elif dimension == "gender":
                    raw_segment = str(item.get("ad_group_criterion.gender.type", "UNDETERMINED") or "")
                    segment = _GOOGLE_GENDER_MAP.get(raw_segment, "unknown")
                else:
                    raw_segment = str(item.get("segments.device", "OTHER") or "")
                    segment = _GOOGLE_DEVICE_MAP.get(raw_segment, "other")
                google_rows.append(
                    {
                        "segment": segment,
                        "impressions": int(item.get("metrics.impressions", 0)),
                        "clicks": int(item.get("metrics.clicks", 0)),
                        "spend_micros": int(item.get("metrics.cost_micros", 0)),
                        "conversions": float(item.get("metrics.conversions", 0)),
                        "conversion_value": float(item.get("metrics.conversions_value", 0) or 0),
                    }
                )

        meta_rows_by_segment: dict[str, list[dict[str, Any]]] = {}
        google_rows_by_segment: dict[str, list[dict[str, Any]]] = {}
        for row in meta_rows:
            meta_rows_by_segment.setdefault(str(row["segment"]), []).append(row)
        for row in google_rows:
            google_rows_by_segment.setdefault(str(row["segment"]), []).append(row)

        segment_keys = sorted(
            set(meta_rows_by_segment.keys()) | set(google_rows_by_segment.keys()),
            key=_device_sort_key if dimension == "device" else (lambda segment: _segment_sort_key(segment, dimension)),
        )

        segments: list[dict[str, Any]] = []
        total_spend_micros = sum(int(r.get("spend_micros", 0)) for r in meta_rows + google_rows)
        for segment in segment_keys:
            meta_segment_rows = meta_rows_by_segment.get(segment, [])
            google_segment_rows = google_rows_by_segment.get(segment, [])
            combined_rows = meta_segment_rows + google_segment_rows
            meta_totals = _aggregate_rows(meta_segment_rows)
            google_totals = _aggregate_rows(google_segment_rows)
            combined_totals = _aggregate_rows(combined_rows)
            if dimension in {"age", "gender"}:
                combined_totals["share_pct"] = round(
                    safe_divide(combined_totals["spend_micros"], total_spend_micros) * 100,
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
        }
        if errors:
            result["errors"] = errors
        attach_diagnostics(result, meta_raw, google_raw, include_raw)
        return json.dumps(result, indent=2)

    meta_by_segment: dict[str, dict[str, Any]] = {}
    google_by_segment: dict[str, dict[str, Any]] = {}

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
            if dimension == "country":
                segment = str(item.get("country", "") or "unknown").upper()
            else:
                segment = str(item.get("publisher_platform", "") or "unknown").lower()
            bucket = meta_by_segment.setdefault(segment, _empty_metrics())
            bucket["impressions"] += int(item.get("impressions", 0) or 0)
            bucket["clicks"] += int(item.get("clicks", 0) or 0)
            bucket["spend_micros"] += meta_spend_to_micros(item.get("spend", 0))
            bucket["conversions"] += _parse_meta_conversions(item)
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
            if dimension == "country":
                segment = _country_from_google_row(item)
            else:
                segment = str(item.get("campaign.advertising_channel_type", "") or "unknown").lower()
            bucket = google_by_segment.setdefault(segment, _empty_metrics())
            bucket["impressions"] += int(item.get("metrics.impressions", 0) or 0)
            bucket["clicks"] += int(item.get("metrics.clicks", 0) or 0)
            bucket["spend_micros"] += int(item.get("metrics.cost_micros", 0) or 0)
            bucket["conversions"] += float(item.get("metrics.conversions", 0) or 0)
            bucket["conversion_value"] += float(item.get("metrics.conversions_value", 0) or 0)

    segment_keys = sorted(set(meta_by_segment.keys()) | set(google_by_segment.keys()))
    segments: list[dict[str, Any]] = []
    for segment in segment_keys:
        meta_metrics = _finalize_metrics(meta_by_segment.get(segment, _empty_metrics()))
        google_metrics = _finalize_metrics(google_by_segment.get(segment, _empty_metrics()))
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

    result = {
        "status": "ok" if not errors else ("partial" if segments else "error"),
        "dimension": dimension,
        "date_start": date_start,
        "date_end": date_end,
        "segments": segments,
    }
    if errors:
        result["errors"] = errors

    attach_diagnostics(result, meta_raw, google_raw, include_raw)

    return json.dumps(result, indent=2)
