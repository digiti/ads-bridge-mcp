import asyncio
import json
from typing import Any

from .. import mcp
from ..client import call_google_tool, call_meta_tool
from ..normalize import meta_spend_to_micros, safe_divide


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


def _extract_meta_campaign_rows(result: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in result.get("data", []):
        impressions = int(item.get("impressions", 0) or 0)
        clicks = int(item.get("clicks", 0) or 0)
        spend_micros = meta_spend_to_micros(item.get("spend", "0"))
        conversions = _extract_meta_conversions(item.get("actions", []))

        ctr = safe_divide(float(clicks), float(impressions)) * 100
        cpc_micros = int(safe_divide(float(spend_micros), float(clicks))) if clicks else 0
        cvr = safe_divide(conversions, float(clicks)) * 100

        rows.append(
            {
                "account_id": item.get("account_id", ""),
                "account_name": item.get("account_name", ""),
                "campaign_id": item.get("campaign_id", ""),
                "campaign_name": item.get("campaign_name", ""),
                "impressions": impressions,
                "clicks": clicks,
                "spend_micros": spend_micros,
                "conversions": conversions,
                "ctr": ctr,
                "cpc_micros": cpc_micros,
                "cvr": cvr,
            }
        )
    return rows


def _is_dismissed(recommendation: dict[str, Any]) -> bool:
    value = recommendation.get("dismissed", False)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() == "true"
    return False


@mcp.tool()
async def get_optimization_opportunities(
    meta_account_ids: list[str],
    google_account_ids: list[str],
    date_start: str,
    date_end: str,
    google_login_customer_id: str | None = None,
) -> str:
    errors: list[dict[str, Any]] = []
    opportunities: list[dict[str, Any]] = []
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
    google_tasks = [
        call_google_tool(
            "get_recommendations",
            {
                "customer_id": account_id,
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

        rows = _extract_meta_campaign_rows(result)
        avg_cpc_micros = int(
            safe_divide(float(sum(int(row.get("spend_micros", 0)) for row in rows)), float(sum(int(row.get("clicks", 0)) for row in rows)))
        )

        for row in rows:
            impressions = int(row.get("impressions", 0))
            clicks = int(row.get("clicks", 0))
            spend_micros = int(row.get("spend_micros", 0))
            conversions = float(row.get("conversions", 0))
            ctr = float(row.get("ctr", 0.0))
            cpc_micros = int(row.get("cpc_micros", 0))
            cvr = float(row.get("cvr", 0.0))

            if ctr < 1.0 and impressions > 1000:
                opportunities.append(
                    {
                        "platform": "meta",
                        "account_id": account_id,
                        "type": "low_ctr",
                        "category": "PERFORMANCE",
                        "priority": "high",
                        "action": "Review ad creative and targeting",
                        "details": {
                            "campaign_id": row.get("campaign_id", ""),
                            "campaign_name": row.get("campaign_name", ""),
                            "ctr": round(ctr, 2),
                            "impressions": impressions,
                        },
                    }
                )

            if avg_cpc_micros > 0 and cpc_micros > avg_cpc_micros * 2:
                opportunities.append(
                    {
                        "platform": "meta",
                        "account_id": account_id,
                        "type": "high_cpc",
                        "category": "COST",
                        "priority": "medium",
                        "action": "Optimize bidding or narrow targeting",
                        "details": {
                            "campaign_id": row.get("campaign_id", ""),
                            "campaign_name": row.get("campaign_name", ""),
                            "cpc_micros": cpc_micros,
                            "account_avg_cpc_micros": avg_cpc_micros,
                        },
                    }
                )

            if cvr < 1.0 and clicks > 100:
                opportunities.append(
                    {
                        "platform": "meta",
                        "account_id": account_id,
                        "type": "low_cvr",
                        "category": "CONVERSIONS",
                        "priority": "high",
                        "action": "Review landing page and conversion setup",
                        "details": {
                            "campaign_id": row.get("campaign_id", ""),
                            "campaign_name": row.get("campaign_name", ""),
                            "cvr": round(cvr, 2),
                            "clicks": clicks,
                        },
                    }
                )

            if spend_micros > 0 and conversions == 0:
                opportunities.append(
                    {
                        "platform": "meta",
                        "account_id": account_id,
                        "type": "no_conversions",
                        "category": "BUDGET",
                        "priority": "high",
                        "action": "Pause or restructure campaign",
                        "details": {
                            "campaign_id": row.get("campaign_id", ""),
                            "campaign_name": row.get("campaign_name", ""),
                            "spend_micros": spend_micros,
                            "conversions": conversions,
                        },
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

        recommendations_data = result.get("recommendations", [])
        if not isinstance(recommendations_data, list):
            recommendations_data = []

        for recommendation in recommendations_data:
            if not isinstance(recommendation, dict):
                continue
            if _is_dismissed(recommendation):
                continue
            opportunities.append(
                {
                    "platform": "google",
                    "account_id": account_id,
                    "type": "recommendation",
                    "category": recommendation.get("type", "UNKNOWN"),
                    "action": "apply_recommendation",
                    "details": recommendation,
                    "priority": "medium",
                }
            )

    priority_order = {"high": 0, "medium": 1, "low": 2}
    sorted_opportunities = sorted(opportunities, key=lambda item: priority_order.get(str(item.get("priority", "low")), 3))

    by_platform = {
        "meta": sum(1 for item in sorted_opportunities if item.get("platform") == "meta"),
        "google": sum(1 for item in sorted_opportunities if item.get("platform") == "google"),
    }
    by_priority = {
        "high": sum(1 for item in sorted_opportunities if item.get("priority") == "high"),
        "medium": sum(1 for item in sorted_opportunities if item.get("priority") == "medium"),
        "low": sum(1 for item in sorted_opportunities if item.get("priority") == "low"),
    }

    result: dict[str, Any] = {
        "status": "ok" if not errors else ("partial" if sorted_opportunities else "error"),
        "date_start": date_start,
        "date_end": date_end,
        "opportunities": sorted_opportunities,
        "count": len(sorted_opportunities),
        "by_platform": by_platform,
        "by_priority": by_priority,
        "platform_results": {"meta": meta_raw, "google": google_raw},
    }
    if errors:
        result["errors"] = errors

    return json.dumps(result, indent=2)
