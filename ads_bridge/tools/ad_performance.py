import asyncio
import json
from typing import Any

from .. import mcp
from ..client import call_google_tool, call_meta_tool
from ..normalize import InvalidDateError, attach_diagnostics, compute_derived_metrics, micros_to_display, normalize_meta_insights, safe_divide, validate_date


def _empty_ad_row() -> dict[str, Any]:
    return {
        "impressions": 0,
        "clicks": 0,
        "spend_micros": 0,
        "conversions": 0.0,
        "conversion_value": 0.0,
    }


def _ad_sort_value(row: dict[str, Any], sort_by: str) -> float:
    value = row.get(sort_by, 0)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _finalize_ad_row(row: dict[str, Any]) -> dict[str, Any]:
    impressions = int(row.get("impressions", 0))
    clicks = int(row.get("clicks", 0))
    spend_micros = int(row.get("spend_micros", 0))
    conversions = float(row.get("conversions", 0))
    conversion_value = float(row.get("conversion_value", 0))
    derived = compute_derived_metrics(impressions, clicks, spend_micros, conversions, conversion_value)

    return {
        **row,
        "impressions": impressions,
        "clicks": clicks,
        "spend_micros": spend_micros,
        "spend": micros_to_display(spend_micros),
        "conversions": round(conversions, 2),
        "conversion_value": round(conversion_value, 2),
        "value_per_spend": round(safe_divide(conversion_value * 1_000_000, spend_micros), 4) if spend_micros else 0.0,
        **derived,
    }


@mcp.tool()
async def compare_ad_performance(
    meta_account_ids: list[str],
    google_account_ids: list[str],
    date_start: str,
    date_end: str,
    google_login_customer_id: str | None = None,
    sort_by: str = "spend_micros",
    limit: int = 20,
    sort_order: str = "desc",
    include_raw: bool = False,
) -> str:
    """Rank ad-level performance across Meta and Google Ads for a date range.

    Use when: You need a cross-platform leaderboard of top or bottom ads by a
    chosen metric (for example spend, clicks, conversions, CTR, or CPC).

    Differs from analyze_creative_performance: creative_performance includes
    creative asset details (headlines, descriptions, images); this tool ranks
    ads purely by performance metrics without creative metadata.

    Args:
        meta_account_ids: Meta ad account IDs to include in ad ranking.
        google_account_ids: Google Ads customer IDs to include in ad ranking.
        date_start: Inclusive start date for ad performance in YYYY-MM-DD format.
        date_end: Inclusive end date for ad performance in YYYY-MM-DD format.
        google_login_customer_id: Optional manager account ID for Google Ads API access.
        sort_by: Metric key used to sort ads across platforms.
        limit: Maximum number of ranked ads to return.
        sort_order: Sort direction, either "asc" or "desc".
    """
    try:
        validate_date(date_start)
        validate_date(date_end)
    except InvalidDateError as exc:
        result = {"status": "error", "ads": [], "errors": [{"source": "validation", "error": str(exc)}]}
        attach_diagnostics(result)
        return json.dumps(result, indent=2)

    if date_start > date_end:
        result = {"status": "error", "ads": [], "errors": [{"source": "validation", "error": f"date_start '{date_start}' is after date_end '{date_end}'"}]}
        attach_diagnostics(result)
        return json.dumps(result, indent=2)

    allowed_sort_by = {"spend_micros", "impressions", "clicks", "conversions", "ctr", "cpc_micros"}
    if sort_by not in allowed_sort_by:
        result = {"status": "error", "ads": [], "errors": [{"source": "validation", "error": f"sort_by must be one of {sorted(allowed_sort_by)}"}]}
        attach_diagnostics(result)
        return json.dumps(result, indent=2)

    allowed_sort_order = {"asc", "desc"}
    if sort_order not in allowed_sort_order:
        result = {"status": "error", "ads": [], "errors": [{"source": "validation", "error": f"sort_order must be one of {sorted(allowed_sort_order)}"}]}
        attach_diagnostics(result)
        return json.dumps(result, indent=2)

    effective_limit = max(int(limit), 0)

    errors: list[dict[str, Any]] = []
    meta_raw: dict[str, Any] = {"accounts": {}}
    google_raw: dict[str, Any] = {"accounts": {}}
    ad_rows: dict[tuple[str, str, str, str], dict[str, Any]] = {}

    meta_tasks = [
        call_meta_tool(
            "get_insights",
            {
                "account_id": account_id,
                "time_range": {"since": date_start, "until": date_end},
                "level": "ad",
            },
        )
        for account_id in meta_account_ids
    ]

    google_fields = [
        "customer.id",
        "customer.descriptive_name",
        "campaign.id",
        "campaign.name",
        "ad_group.id",
        "ad_group.name",
        "ad_group_ad.ad.id",
        "ad_group_ad.ad.name",
        "ad_group_ad.ad.type",
        "ad_group_ad.status",
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
                "resource": "ad_group_ad",
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

        for row in normalize_meta_insights(result):
            ad_id = str(row.get("ad_id", "") or "")
            ad_name = str(row.get("ad_name", "") or "")
            key_ad_id = ad_id or ad_name or f"meta-{row.get('campaign_id', '')}-{row.get('date_start', '')}"
            key = (
                "meta",
                str(row.get("account_id", "")),
                key_ad_id,
                str(row.get("campaign_id", "")),
            )
            bucket = ad_rows.setdefault(
                key,
                {
                    "platform": "meta",
                    "account_id": str(row.get("account_id", "")),
                    "account_name": str(row.get("account_name", "")),
                    "campaign_id": str(row.get("campaign_id", "")),
                    "campaign_name": str(row.get("campaign_name", "")),
                    "ad_id": key_ad_id,
                    "ad_name": ad_name,
                    "ad_type": str(row.get("ad_type", "")),
                    "status": str(row.get("status", "")),
                    **_empty_ad_row(),
                },
            )
            bucket["impressions"] += int(row.get("impressions", 0) or 0)
            bucket["clicks"] += int(row.get("clicks", 0) or 0)
            bucket["spend_micros"] += int(row.get("spend_micros", 0) or 0)
            bucket["conversions"] += float(row.get("conversions", 0) or 0)
            bucket["conversion_value"] += float(row.get("conversion_value", 0) or 0)

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
            ad_id = str(item.get("ad_group_ad.ad.id", "") or "")
            ad_name = str(item.get("ad_group_ad.ad.name", "") or "")
            fallback_id = ad_id or ad_name or f"google-{item.get('campaign.id', '')}-{item.get('ad_group.id', '')}"
            key = (
                "google",
                str(item.get("customer.id", "")),
                fallback_id,
                str(item.get("campaign.id", "")),
            )
            bucket = ad_rows.setdefault(
                key,
                {
                    "platform": "google",
                    "account_id": str(item.get("customer.id", "")),
                    "account_name": str(item.get("customer.descriptive_name", "")),
                    "campaign_id": str(item.get("campaign.id", "")),
                    "campaign_name": str(item.get("campaign.name", "")),
                    "ad_group_id": str(item.get("ad_group.id", "")),
                    "ad_group_name": str(item.get("ad_group.name", "")),
                    "ad_id": fallback_id,
                    "ad_name": ad_name,
                    "ad_type": str(item.get("ad_group_ad.ad.type", "")),
                    "status": str(item.get("ad_group_ad.status", "")),
                    **_empty_ad_row(),
                },
            )
            bucket["impressions"] += int(item.get("metrics.impressions", 0) or 0)
            bucket["clicks"] += int(item.get("metrics.clicks", 0) or 0)
            bucket["spend_micros"] += int(item.get("metrics.cost_micros", 0) or 0)
            bucket["conversions"] += float(item.get("metrics.conversions", 0) or 0)
            bucket["conversion_value"] += float(item.get("metrics.conversions_value", 0) or 0)

    finalized_ads = [_finalize_ad_row(row) for row in ad_rows.values()]
    reverse_sort = sort_order == "desc"
    ranked = sorted(finalized_ads, key=lambda row: _ad_sort_value(row, sort_by), reverse=reverse_sort)
    selected = ranked[:effective_limit]

    output_ads = [
        {
            "rank": index + 1,
            "platform": row.get("platform", ""),
            "account_id": row.get("account_id", ""),
            "account_name": row.get("account_name", ""),
            "campaign_id": row.get("campaign_id", ""),
            "campaign_name": row.get("campaign_name", ""),
            "ad_id": row.get("ad_id", ""),
            "ad_name": row.get("ad_name", ""),
            "impressions": int(row.get("impressions", 0)),
            "clicks": int(row.get("clicks", 0)),
            "spend_micros": int(row.get("spend_micros", 0)),
            "spend": row.get("spend", "0.00"),
            "conversions": float(row.get("conversions", 0)),
            "ctr": float(row.get("ctr", 0)),
            "cpc_micros": int(row.get("cpc_micros", 0)),
            "cpm_micros": int(row.get("cpm_micros", 0)),
            "sort_value": _ad_sort_value(row, sort_by),
        }
        for index, row in enumerate(selected)
    ]

    status = "ok" if not errors else ("partial" if output_ads else "error")
    result: dict[str, Any] = {
        "status": status,
        "date_start": date_start,
        "date_end": date_end,
        "sort_by": sort_by,
        "sort_order": sort_order,
        "limit": effective_limit,
        "ads": output_ads,
        "total_ads_found": len(finalized_ads),
    }
    if errors:
        result["errors"] = errors

    attach_diagnostics(result, meta_raw, google_raw, include_raw)

    return json.dumps(result, indent=2)
