import asyncio
import json
from typing import Any

from .. import mcp
from ..client import call_google_tool, call_meta_tool
from ..normalize import meta_spend_to_micros, micros_to_display, safe_divide


def _extract_meta_conversions(actions: Any) -> float:
    if not isinstance(actions, list):
        return 0.0
    conversions = 0.0
    for action in actions:
        if not isinstance(action, dict):
            continue
        if action.get("action_type") in ("purchase", "lead", "complete_registration", "omni_purchase"):
            conversions += float(action.get("value", 0) or 0)
    return conversions


def _sort_metric_value(item: dict[str, Any], sort_by: str) -> float:
    performance = item.get("performance", {})
    if isinstance(performance, dict) and sort_by in performance:
        try:
            return float(performance.get(sort_by, 0) or 0)
        except (TypeError, ValueError):
            return 0.0
    try:
        return float(item.get(sort_by, 0) or 0)
    except (TypeError, ValueError):
        return 0.0


def _extract_meta_creative_fields(creative_payload: dict[str, Any]) -> dict[str, Any]:
    link_data = {}
    object_story_spec = creative_payload.get("object_story_spec", {})
    if isinstance(object_story_spec, dict):
        link_data_raw = object_story_spec.get("link_data", {})
        if isinstance(link_data_raw, dict):
            link_data = link_data_raw

    asset_feed_spec = creative_payload.get("asset_feed_spec", {})
    if not isinstance(asset_feed_spec, dict):
        asset_feed_spec = {}

    headlines = asset_feed_spec.get("titles", [])
    bodies = asset_feed_spec.get("bodies", [])

    headline = str(link_data.get("name", ""))
    body = str(link_data.get("message", ""))
    cta = str(link_data.get("call_to_action", {}).get("type", "")) if isinstance(link_data.get("call_to_action"), dict) else ""
    link = str(link_data.get("link", ""))

    if not headline and isinstance(headlines, list) and headlines:
        first_headline = headlines[0]
        if isinstance(first_headline, dict):
            headline = str(first_headline.get("text", ""))

    if not body and isinstance(bodies, list) and bodies:
        first_body = bodies[0]
        if isinstance(first_body, dict):
            body = str(first_body.get("text", ""))

    return {
        "type": "image",
        "thumbnail_url": creative_payload.get("thumbnail_url", ""),
        "image_url": creative_payload.get("image_url", ""),
        "headline": headline,
        "body": body,
        "cta": cta,
        "link": link,
    }


def _extract_google_text_assets(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    items: list[str] = []
    for entry in value:
        if isinstance(entry, dict):
            if "text" in entry:
                items.append(str(entry.get("text", "")))
            elif "asset.text" in entry:
                items.append(str(entry.get("asset.text", "")))
        elif isinstance(entry, str):
            items.append(entry)
    return [item for item in items if item]


@mcp.tool()
async def analyze_creative_performance(
    meta_account_ids: list[str],
    google_account_ids: list[str],
    date_start: str,
    date_end: str,
    google_login_customer_id: str | None = None,
    limit: int = 10,
    sort_by: str = "spend_micros",
) -> str:
    """Analyze top-performing creatives and ad assets across Meta and Google Ads.

    Use when: You need creative-level insights (headlines, descriptions, URLs,
    and media metadata) tied to performance metrics to guide copy or asset iteration.

    Args:
        meta_account_ids: Meta ad account IDs to analyze for creative performance.
        google_account_ids: Google Ads customer IDs to analyze for creative performance.
        date_start: Inclusive start date for creative performance in YYYY-MM-DD format.
        date_end: Inclusive end date for creative performance in YYYY-MM-DD format.
        google_login_customer_id: Optional manager account ID for Google Ads API access.
        limit: Maximum creatives to keep per account before final ranking.
        sort_by: Metric key used to rank returned creatives.
    """
    errors: list[dict[str, Any]] = []
    creatives: list[dict[str, Any]] = []
    meta_raw: dict[str, Any] = {"insights": {}, "creatives": {}}
    google_raw: dict[str, Any] = {"ads": {}}
    total_ads_analyzed = {"meta": 0, "google": 0}

    meta_insight_tasks = [
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
        "ad_group_ad.ad.responsive_search_ad.headlines",
        "ad_group_ad.ad.responsive_search_ad.descriptions",
        "ad_group_ad.ad.final_urls",
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

    meta_insight_results = await asyncio.gather(*meta_insight_tasks, return_exceptions=True)
    google_results = await asyncio.gather(*google_tasks, return_exceptions=True)

    meta_top_ads: list[dict[str, Any]] = []

    for idx, account_id in enumerate(meta_account_ids):
        raw_result = meta_insight_results[idx]
        if isinstance(raw_result, BaseException):
            message = str(raw_result)
            errors.append({"platform": "meta", "account_id": account_id, "error": message})
            meta_raw["insights"][account_id] = {"error": message}
            continue
        if not isinstance(raw_result, dict):
            message = f"Unexpected Meta response type: {type(raw_result).__name__}"
            errors.append({"platform": "meta", "account_id": account_id, "error": message})
            meta_raw["insights"][account_id] = {"error": message}
            continue

        result = raw_result
        meta_raw["insights"][account_id] = result
        if "error" in result:
            errors.append({"platform": "meta", "account_id": account_id, "error": str(result["error"])})
            continue

        ad_rows: list[dict[str, Any]] = []
        for item in result.get("data", []):
            if not isinstance(item, dict):
                continue
            impressions = int(item.get("impressions", 0) or 0)
            clicks = int(item.get("clicks", 0) or 0)
            spend_micros = meta_spend_to_micros(item.get("spend", "0"))
            conversions = _extract_meta_conversions(item.get("actions", []))
            ctr = round(safe_divide(float(clicks), float(impressions)) * 100, 2)
            cpc_micros = int(safe_divide(float(spend_micros), float(clicks))) if clicks else 0
            cvr = round(safe_divide(conversions, float(clicks)) * 100, 2)

            ad_rows.append(
                {
                    "platform": "meta",
                    "account_id": item.get("account_id", account_id),
                    "account_name": item.get("account_name", ""),
                    "ad_id": item.get("ad_id", ""),
                    "ad_name": item.get("ad_name", ""),
                    "campaign_name": item.get("campaign_name", ""),
                    "performance": {
                        "impressions": impressions,
                        "clicks": clicks,
                        "spend_micros": spend_micros,
                        "spend": micros_to_display(spend_micros),
                        "ctr": ctr,
                        "cpc_micros": cpc_micros,
                        "conversions": round(conversions, 2),
                        "cvr": cvr,
                    },
                }
            )

        total_ads_analyzed["meta"] += len(ad_rows)
        ordered = sorted(ad_rows, key=lambda row: _sort_metric_value(row, sort_by), reverse=True)
        meta_top_ads.extend(ordered[: max(limit, 0)])

    creative_tasks = [
        call_meta_tool("get_ad_creatives", {"ad_id": str(ad.get("ad_id", ""))})
        for ad in meta_top_ads
        if str(ad.get("ad_id", ""))
    ]
    creative_results = await asyncio.gather(*creative_tasks, return_exceptions=True)

    creative_idx = 0
    for ad in meta_top_ads:
        ad_id = str(ad.get("ad_id", ""))
        if not ad_id:
            continue

        raw_creative = creative_results[creative_idx]
        creative_idx += 1

        account_id = str(ad.get("account_id", ""))

        if isinstance(raw_creative, BaseException):
            message = str(raw_creative)
            errors.append({"platform": "meta", "account_id": account_id, "ad_id": ad_id, "error": message})
            meta_raw["creatives"][ad_id] = {"error": message}
            continue
        if not isinstance(raw_creative, dict):
            message = f"Unexpected Meta response type: {type(raw_creative).__name__}"
            errors.append({"platform": "meta", "account_id": account_id, "ad_id": ad_id, "error": message})
            meta_raw["creatives"][ad_id] = {"error": message}
            continue

        creative_result = raw_creative
        meta_raw["creatives"][ad_id] = creative_result
        if "error" in creative_result:
            errors.append(
                {
                    "platform": "meta",
                    "account_id": account_id,
                    "ad_id": ad_id,
                    "error": str(creative_result["error"]),
                }
            )
            continue

        creative_payload = {}
        if isinstance(creative_result.get("data"), list) and creative_result.get("data"):
            first = creative_result["data"][0]
            if isinstance(first, dict):
                creative_payload = first
        elif isinstance(creative_result.get("creative"), dict):
            creative_payload = creative_result["creative"]
        elif isinstance(creative_result, dict):
            creative_payload = creative_result

        creatives.append(
            {
                "platform": "meta",
                "account_id": account_id,
                "account_name": ad.get("account_name", ""),
                "ad_id": ad_id,
                "ad_name": ad.get("ad_name", ""),
                "campaign_name": ad.get("campaign_name", ""),
                "creative": _extract_meta_creative_fields(creative_payload),
                "performance": ad.get("performance", {}),
            }
        )

    for idx, account_id in enumerate(google_account_ids):
        raw_result = google_results[idx]
        if isinstance(raw_result, BaseException):
            message = str(raw_result)
            errors.append({"platform": "google", "account_id": account_id, "error": message})
            google_raw["ads"][account_id] = {"error": message}
            continue
        if not isinstance(raw_result, dict):
            message = f"Unexpected Google response type: {type(raw_result).__name__}"
            errors.append({"platform": "google", "account_id": account_id, "error": message})
            google_raw["ads"][account_id] = {"error": message}
            continue

        result = raw_result
        google_raw["ads"][account_id] = result
        if "error" in result:
            errors.append({"platform": "google", "account_id": account_id, "error": str(result["error"])})
            continue

        ad_map: dict[str, dict[str, Any]] = {}
        for row in result.get("data", []):
            if not isinstance(row, dict):
                continue

            ad_id = str(row.get("ad_group_ad.ad.id", ""))
            if not ad_id:
                continue

            if ad_id not in ad_map:
                ad_map[ad_id] = {
                    "platform": "google",
                    "account_id": row.get("customer.id", account_id),
                    "account_name": row.get("customer.descriptive_name", ""),
                    "ad_id": ad_id,
                    "ad_name": row.get("ad_group_ad.ad.name", ""),
                    "campaign_name": row.get("campaign.name", ""),
                    "creative": {
                        "type": "responsive_search_ad",
                        "headlines": _extract_google_text_assets(row.get("ad_group_ad.ad.responsive_search_ad.headlines", [])),
                        "descriptions": _extract_google_text_assets(
                            row.get("ad_group_ad.ad.responsive_search_ad.descriptions", [])
                        ),
                        "final_urls": [str(url) for url in row.get("ad_group_ad.ad.final_urls", []) if str(url)],
                    },
                    "performance": {
                        "impressions": 0,
                        "clicks": 0,
                        "spend_micros": 0,
                        "conversions": 0.0,
                    },
                }

            ad_map[ad_id]["performance"]["impressions"] += int(row.get("metrics.impressions", 0) or 0)
            ad_map[ad_id]["performance"]["clicks"] += int(row.get("metrics.clicks", 0) or 0)
            ad_map[ad_id]["performance"]["spend_micros"] += int(row.get("metrics.cost_micros", 0) or 0)
            ad_map[ad_id]["performance"]["conversions"] += float(row.get("metrics.conversions", 0) or 0)

        google_ads = []
        for ad in ad_map.values():
            impressions = int(ad["performance"].get("impressions", 0))
            clicks = int(ad["performance"].get("clicks", 0))
            spend_micros = int(ad["performance"].get("spend_micros", 0))
            conversions = float(ad["performance"].get("conversions", 0))
            ad["performance"]["spend"] = micros_to_display(spend_micros)
            ad["performance"]["ctr"] = round(safe_divide(float(clicks), float(impressions)) * 100, 2)
            ad["performance"]["cpc_micros"] = int(safe_divide(float(spend_micros), float(clicks))) if clicks else 0
            ad["performance"]["conversions"] = round(conversions, 2)
            ad["performance"]["cvr"] = round(safe_divide(conversions, float(clicks)) * 100, 2)
            google_ads.append(ad)

        total_ads_analyzed["google"] += len(google_ads)
        ordered = sorted(google_ads, key=lambda row: _sort_metric_value(row, sort_by), reverse=True)
        creatives.extend(ordered[: max(limit, 0)])

    ordered_creatives = sorted(creatives, key=lambda row: _sort_metric_value(row, sort_by), reverse=True)
    ranked_creatives = [{"rank": idx + 1, **creative} for idx, creative in enumerate(ordered_creatives)]

    result: dict[str, Any] = {
        "status": "ok" if not errors else ("partial" if ranked_creatives else "error"),
        "date_start": date_start,
        "date_end": date_end,
        "sort_by": sort_by,
        "limit": limit,
        "creatives": ranked_creatives,
        "total_ads_analyzed": total_ads_analyzed,
    }
    if errors:
        result["errors"] = errors

    return json.dumps(result, indent=2)
