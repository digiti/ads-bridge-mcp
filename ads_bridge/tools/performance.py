import asyncio
import json
from typing import Any

from .. import mcp
from ..client import call_google_tool, call_meta_tool
from ..normalize import (
    InvalidDateError,
    attach_diagnostics,
    build_response,
    compute_derived_metrics,
    micros_to_display,
    normalize_google_insights,
    normalize_meta_insights,
    safe_divide,
    validate_date,
)


def _aggregate_rows(rows: list[dict[str, Any]], aggregation: str) -> list[dict[str, Any]]:
    def base_row(source_rows: list[dict[str, Any]], label: dict[str, Any]) -> dict[str, Any]:
        impressions = sum(int(r.get("impressions", 0)) for r in source_rows)
        clicks = sum(int(r.get("clicks", 0)) for r in source_rows)
        spend_micros = sum(int(r.get("spend_micros", 0)) for r in source_rows)
        conversions = sum(float(r.get("conversions", 0)) for r in source_rows)
        conversion_value = sum(float(r.get("conversion_value", 0)) for r in source_rows)
        derived = compute_derived_metrics(impressions, clicks, spend_micros, conversions, conversion_value)
        return {
            **label,
            "impressions": impressions,
            "clicks": clicks,
            "spend_micros": spend_micros,
            "spend": micros_to_display(spend_micros),
            "conversions": round(conversions, 2),
            "conversion_value": round(conversion_value, 2),
            **derived,
        }

    if aggregation == "total":
        return [base_row(rows, {"aggregation": "total"})]

    if aggregation == "by_account":
        account_buckets: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in rows:
            key = (str(row.get("platform", "")), str(row.get("account_id", "")))
            account_buckets.setdefault(key, []).append(row)

        out: list[dict[str, Any]] = []
        for (platform, account_id), bucket_rows in account_buckets.items():
            out.append(
                base_row(
                    bucket_rows,
                    {
                        "aggregation": "by_account",
                        "platform": platform,
                        "account_id": account_id,
                        "account_name": bucket_rows[0].get("account_name", ""),
                    },
                )
            )
        return sorted(out, key=lambda r: (str(r.get("platform", "")), str(r.get("account_name", ""))))

    platform_buckets: dict[str, list[dict[str, Any]]] = {"meta": [], "google": []}
    for row in rows:
        platform = str(row.get("platform", ""))
        platform_buckets.setdefault(platform, []).append(row)

    out = []
    for platform, platform_rows in platform_buckets.items():
        if not platform_rows:
            continue
        out.append(base_row(platform_rows, {"aggregation": "by_platform", "platform": platform}))
    return sorted(out, key=lambda r: str(r.get("platform", "")))


def _top_campaign_rows(rows: list[dict[str, Any]], sort_by: str, limit: int) -> list[dict[str, Any]]:
    metric_key = {
        "spend": "spend_micros",
        "impressions": "impressions",
        "clicks": "clicks",
        "conversions": "conversions",
    }[sort_by]

    ranked = sorted(rows, key=lambda row: float(row.get(metric_key, 0)), reverse=True)[: max(limit, 0)]
    return [
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


def _summary_top_campaigns(rows: list[dict[str, Any]], sort_by: str, limit: int = 3) -> list[dict[str, Any]]:
    metric_key = {
        "spend": "spend_micros",
        "impressions": "impressions",
        "clicks": "clicks",
        "conversions": "conversions",
    }[sort_by]
    ordered = sorted(rows, key=lambda row: float(row.get(metric_key, 0)), reverse=True)
    return [
        {
            "platform": row.get("platform", ""),
            "campaign_id": row.get("campaign_id", ""),
            "campaign_name": row.get("campaign_name", ""),
            "account_id": row.get("account_id", ""),
            "account_name": row.get("account_name", ""),
            "spend_micros": int(row.get("spend_micros", 0)),
            "spend": micros_to_display(int(row.get("spend_micros", 0))),
            "impressions": int(row.get("impressions", 0)),
            "clicks": int(row.get("clicks", 0)),
            "conversions": float(row.get("conversions", 0)),
        }
        for row in ordered[:limit]
    ]


@mcp.tool()
async def compare_performance(
    meta_account_ids: list[str],
    google_account_ids: list[str],
    date_start: str,
    date_end: str,
    google_login_customer_id: str | None = None,
    aggregation: str = "by_platform",
    level: str = "campaign",
    sort_by: str = "spend",
    limit: int = 10,
    include_raw: bool = False,
) -> str:
    """Compare Meta and Google performance across multiple aggregation modes.

    Use when: You need a unified cross-platform performance view at any granularity
    (platform, account, campaign ranking, or executive summary) over a custom date range.

    Aggregation modes:
    - by_platform: One row per platform with totals.
    - by_account: One row per ad account across both platforms.
    - total: Single combined row for all accounts.
    - top_campaigns: Rank individual campaigns by sort_by, return top limit rows.
    - summary: Executive view with totals, platform split percentages, and top 3 campaigns per platform.

    Differs from compare_daily_trends: daily_trends shows day-by-day timeline;
    this tool aggregates the full range into rollup rows.
    Differs from get_period_comparison: period_comparison computes deltas between
    two date ranges; this tool shows a single range.
    Differs from get_budget_analysis(allocation): budget_analysis focuses on spend
    split and ROAS recommendation; this tool provides full metric breakdowns.

    Args:
        meta_account_ids: Meta ad account IDs to query.
        google_account_ids: Google Ads customer IDs to query.
        date_start: Inclusive start date in YYYY-MM-DD.
        date_end: Inclusive end date in YYYY-MM-DD.
        google_login_customer_id: Optional manager account ID for cross-account querying.
        aggregation: Rollup mode (by_platform, by_account, total, top_campaigns, summary).
        level: Data level for API queries (campaign or account).
        sort_by: Metric to rank by when aggregation is top_campaigns or summary.
        limit: Max rows for top_campaigns mode.
    """

    try:
        validate_date(date_start)
        validate_date(date_end)
    except InvalidDateError as exc:
        result = {"status": "error", "rows": [], "errors": [{"source": "validation", "error": str(exc)}]}
        attach_diagnostics(result)
        return json.dumps(result, indent=2)

    if date_start > date_end:
        result = {"status": "error", "rows": [], "errors": [{"source": "validation", "error": f"date_start '{date_start}' is after date_end '{date_end}'"}]}
        attach_diagnostics(result)
        return json.dumps(result, indent=2)

    allowed_aggregations = {"by_platform", "by_account", "total", "top_campaigns", "summary"}
    allowed_levels = {"account", "campaign"}
    allowed_sort = {"spend", "impressions", "clicks", "conversions"}

    if aggregation not in allowed_aggregations:
        result = {"status": "error", "rows": [], "errors": [{"source": "validation", "error": f"aggregation must be one of {sorted(allowed_aggregations)}"}]}
        attach_diagnostics(result)
        return json.dumps(result, indent=2)

    if level not in allowed_levels:
        result = {"status": "error", "rows": [], "errors": [{"source": "validation", "error": f"level must be one of {sorted(allowed_levels)}"}]}
        attach_diagnostics(result)
        return json.dumps(result, indent=2)

    if sort_by not in allowed_sort:
        result = {"status": "error", "rows": [], "errors": [{"source": "validation", "error": f"sort_by must be one of {sorted(allowed_sort)}"}]}
        attach_diagnostics(result)
        return json.dumps(result, indent=2)

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
                "level": level,
            },
        )
        for account_id in meta_account_ids
    ]

    google_conditions = [f"segments.date BETWEEN '{date_start}' AND '{date_end}'"]
    google_resource = "campaign" if level == "campaign" else "customer"
    google_fields = (
        [
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
        if level == "campaign"
        else [
            "customer.id",
            "customer.descriptive_name",
            "metrics.impressions",
            "metrics.clicks",
            "metrics.cost_micros",
            "metrics.conversions",
            "metrics.conversions_value",
            "segments.date",
        ]
    )
    google_tasks = [
        call_google_tool(
            "search_ads",
            {
                "customer_id": account_id,
                "resource": google_resource,
                "fields": google_fields,
                "conditions": google_conditions,
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

    all_rows = meta_rows + google_rows

    if aggregation in {"by_platform", "by_account", "total"}:
        aggregated_rows = _aggregate_rows(all_rows, aggregation)
        status = "ok" if not errors else ("partial" if aggregated_rows else "error")

        response = build_response(
            status=status,
            rows=aggregated_rows,
            errors=errors or None,
            meta_raw=meta_raw,
            google_raw=google_raw,
            include_raw=include_raw,
        )
        response["metadata"] = {
            "date_start": date_start,
            "date_end": date_end,
            "aggregation": aggregation,
            "source_row_count": len(all_rows),
        }
        return json.dumps(response, indent=2)

    if aggregation == "top_campaigns":
        output_rows = _top_campaign_rows(all_rows, sort_by, limit)
        result: dict[str, Any] = {
            "status": "ok" if not errors else ("partial" if output_rows else "error"),
            "sort_by": sort_by,
            "limit": limit,
            "date_start": date_start,
            "date_end": date_end,
            "rows": output_rows,
        }
        if errors:
            result["errors"] = errors
        attach_diagnostics(result, meta_raw, google_raw, include_raw)
        return json.dumps(result, indent=2)

    total_spend_micros = sum(int(row.get("spend_micros", 0)) for row in all_rows)
    total_impressions = sum(int(row.get("impressions", 0)) for row in all_rows)
    total_clicks = sum(int(row.get("clicks", 0)) for row in all_rows)
    total_conversions = sum(float(row.get("conversions", 0)) for row in all_rows)
    total_conversion_value = sum(float(row.get("conversion_value", 0)) for row in all_rows)

    meta_spend = sum(int(row.get("spend_micros", 0)) for row in meta_rows)
    google_spend = sum(int(row.get("spend_micros", 0)) for row in google_rows)

    derived = compute_derived_metrics(
        total_impressions,
        total_clicks,
        total_spend_micros,
        total_conversions,
        total_conversion_value,
    )
    spend_split = {
        "meta_pct": round(safe_divide(meta_spend, total_spend_micros) * 100, 2),
        "google_pct": round(safe_divide(google_spend, total_spend_micros) * 100, 2),
    }

    summary_result: dict[str, Any] = {
        "status": "ok" if not errors else ("partial" if all_rows else "error"),
        "date_start": date_start,
        "date_end": date_end,
        "totals": {
            "spend_micros": total_spend_micros,
            "spend": micros_to_display(total_spend_micros),
            "impressions": total_impressions,
            "clicks": total_clicks,
            "conversions": round(total_conversions, 2),
            "conversion_value": round(total_conversion_value, 2),
            **derived,
        },
        "platform_split": {
            "meta": {"spend_micros": meta_spend, "spend": micros_to_display(meta_spend), "pct": spend_split["meta_pct"]},
            "google": {
                "spend_micros": google_spend,
                "spend": micros_to_display(google_spend),
                "pct": spend_split["google_pct"],
            },
        },
        "top_campaigns": {
            "meta": _summary_top_campaigns(meta_rows, sort_by, 3),
            "google": _summary_top_campaigns(google_rows, sort_by, 3),
        },
    }
    if errors:
        summary_result["errors"] = errors

    attach_diagnostics(summary_result, meta_raw, google_raw, include_raw)

    return json.dumps(summary_result, indent=2)
