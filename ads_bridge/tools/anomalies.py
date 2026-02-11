import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Any

from .. import mcp
from ..client import call_google_tool, call_meta_tool
from ..normalize import normalize_google_insights, normalize_meta_insights, safe_divide


def _group_by_campaign(rows: list[dict[str, Any]]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = (str(row.get("platform", "")), str(row.get("campaign_id", "")))
        grouped.setdefault(key, []).append(row)
    return grouped


def _daily_rollup(campaign_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_date: dict[str, dict[str, Any]] = {}
    for row in campaign_rows:
        date_key = str(row.get("date_start") or row.get("date_stop") or "")
        if date_key not in by_date:
            by_date[date_key] = {
                "date": date_key,
                "spend_micros": 0,
                "impressions": 0,
                "clicks": 0,
                "conversions": 0.0,
            }
        by_date[date_key]["spend_micros"] += int(row.get("spend_micros", 0))
        by_date[date_key]["impressions"] += int(row.get("impressions", 0))
        by_date[date_key]["clicks"] += int(row.get("clicks", 0))
        by_date[date_key]["conversions"] += float(row.get("conversions", 0))

    ordered = [by_date[d] for d in sorted(by_date.keys()) if d]
    for day in ordered:
        day["ctr"] = safe_divide(float(day["clicks"]), float(day["impressions"])) * 100
    return ordered


def _analyze_campaign(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    daily = _daily_rollup(rows)
    if len(daily) < 7:
        return []

    previous = daily[:5]
    recent = daily[5:7]

    prev_spend = safe_divide(sum(float(d["spend_micros"]) for d in previous), len(previous))
    prev_ctr = safe_divide(sum(float(d["ctr"]) for d in previous), len(previous))
    prev_conv = safe_divide(sum(float(d["conversions"]) for d in previous), len(previous))

    recent_spend = safe_divide(sum(float(d["spend_micros"]) for d in recent), len(recent))
    recent_ctr = safe_divide(sum(float(d["ctr"]) for d in recent), len(recent))
    recent_conv = safe_divide(sum(float(d["conversions"]) for d in recent), len(recent))

    anomalies: list[dict[str, Any]] = []

    spend_change_pct = safe_divide((recent_spend - prev_spend), prev_spend) * 100 if prev_spend else 0.0
    if spend_change_pct > 100:
        anomalies.append(
            {
                "metric": "spend",
                "direction": "increase",
                "expected": round(prev_spend, 2),
                "actual": round(recent_spend, 2),
                "change_pct": round(spend_change_pct, 2),
                "severity": "high" if spend_change_pct > 200 else "medium",
            }
        )

    ctr_change_pct = safe_divide((recent_ctr - prev_ctr), prev_ctr) * 100 if prev_ctr else 0.0
    if ctr_change_pct < -30:
        anomalies.append(
            {
                "metric": "ctr",
                "direction": "decrease",
                "expected": round(prev_ctr, 2),
                "actual": round(recent_ctr, 2),
                "change_pct": round(ctr_change_pct, 2),
                "severity": "high" if ctr_change_pct < -50 else "medium",
            }
        )

    conv_change_pct = safe_divide((recent_conv - prev_conv), prev_conv) * 100 if prev_conv else 0.0
    if conv_change_pct < -50:
        anomalies.append(
            {
                "metric": "conversions",
                "direction": "decrease",
                "expected": round(prev_conv, 2),
                "actual": round(recent_conv, 2),
                "change_pct": round(conv_change_pct, 2),
                "severity": "high" if conv_change_pct < -70 else "medium",
            }
        )

    if not anomalies:
        return []

    representative = rows[0]
    return [
        {
            "platform": representative.get("platform", ""),
            "account_id": representative.get("account_id", ""),
            "account_name": representative.get("account_name", ""),
            "campaign_id": representative.get("campaign_id", ""),
            "campaign_name": representative.get("campaign_name", ""),
            "window": {
                "previous_days": [previous[0]["date"], previous[-1]["date"]],
                "recent_days": [recent[0]["date"], recent[-1]["date"]],
            },
            **item,
        }
        for item in anomalies
    ]


@mcp.tool()
async def detect_anomalies(
    meta_account_ids: list[str],
    google_account_ids: list[str],
    google_login_customer_id: str | None = None,
) -> str:
    today = datetime.now(UTC).date()
    date_start = (today - timedelta(days=6)).isoformat()
    date_end = today.isoformat()

    errors: list[dict[str, Any]] = []
    meta_raw: dict[str, Any] = {"accounts": {}}
    google_raw: dict[str, Any] = {"accounts": {}}
    all_rows: list[dict[str, Any]] = []

    meta_tasks = [
        call_meta_tool(
            "get_insights",
            {
                "account_id": account_id,
                "time_range": {"since": date_start, "until": date_end},
                "level": "campaign",
                "time_increment": 1,
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
        all_rows.extend(normalize_meta_insights(result))

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
        all_rows.extend(normalize_google_insights(result))

    grouped = _group_by_campaign(all_rows)
    anomalies: list[dict[str, Any]] = []
    for campaign_rows in grouped.values():
        anomalies.extend(_analyze_campaign(campaign_rows))

    result = {
        "status": "ok" if not errors else ("partial" if anomalies else "error"),
        "date_start": date_start,
        "date_end": date_end,
        "thresholds": {
            "spend_spike_pct": 100,
            "ctr_drop_pct": -30,
            "conversions_drop_pct": -50,
        },
        "anomalies": anomalies,
        "count": len(anomalies),
        "platform_results": {"meta": meta_raw, "google": google_raw},
    }
    if errors:
        result["errors"] = errors

    return json.dumps(result, indent=2)
