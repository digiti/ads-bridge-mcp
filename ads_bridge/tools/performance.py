import asyncio
import json
from typing import Any

from .. import mcp
from ..client import call_google_tool, call_meta_tool
from ..normalize import (
    build_response,
    compute_derived_metrics,
    micros_to_display,
    normalize_google_insights,
    normalize_meta_insights,
)


def _aggregate_rows(rows: list[dict[str, Any]], aggregation: str) -> list[dict[str, Any]]:
    def base_row(source_rows: list[dict[str, Any]], label: dict[str, Any]) -> dict[str, Any]:
        impressions = sum(int(r.get("impressions", 0)) for r in source_rows)
        clicks = sum(int(r.get("clicks", 0)) for r in source_rows)
        spend_micros = sum(int(r.get("spend_micros", 0)) for r in source_rows)
        conversions = sum(float(r.get("conversions", 0)) for r in source_rows)
        conversion_value = sum(float(r.get("conversion_value", 0)) for r in source_rows)
        derived = compute_derived_metrics(impressions, clicks, spend_micros, conversions)
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


@mcp.tool()
async def compare_performance(
    meta_account_ids: list[str],
    google_account_ids: list[str],
    date_start: str,
    date_end: str,
    google_login_customer_id: str | None = None,
    aggregation: str = "by_platform",
) -> str:
    allowed_aggregations = {"by_platform", "by_account", "total"}
    if aggregation not in allowed_aggregations:
        return json.dumps(
            {
                "status": "error",
                "rows": [],
                "errors": [
                    {
                        "source": "validation",
                        "error": f"aggregation must be one of {sorted(allowed_aggregations)}",
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
            },
        )
        for account_id in meta_account_ids
    ]

    google_conditions = [f"segments.date BETWEEN '{date_start}' AND '{date_end}'"]
    google_fields = [
        "customer.id",
        "customer.descriptive_name",
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
                "resource": "customer",
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
        result = meta_results[idx]
        if isinstance(result, BaseException):
            message = str(result)
            errors.append({"platform": "meta", "account_id": account_id, "error": message})
            meta_raw["accounts"][account_id] = {"error": message}
            continue

        meta_raw["accounts"][account_id] = result
        if "error" in result:
            errors.append({"platform": "meta", "account_id": account_id, "error": str(result["error"])})
            continue
        meta_rows.extend(normalize_meta_insights(result))

    for idx, account_id in enumerate(google_account_ids):
        result = google_results[idx]
        if isinstance(result, BaseException):
            message = str(result)
            errors.append({"platform": "google", "account_id": account_id, "error": message})
            google_raw["accounts"][account_id] = {"error": message}
            continue

        google_raw["accounts"][account_id] = result
        if "error" in result:
            errors.append({"platform": "google", "account_id": account_id, "error": str(result["error"])})
            continue
        google_rows.extend(normalize_google_insights(result))

    all_rows = meta_rows + google_rows
    aggregated_rows = _aggregate_rows(all_rows, aggregation)
    status = "ok" if not errors else ("partial" if aggregated_rows else "error")

    response = build_response(
        status=status,
        rows=aggregated_rows,
        meta_raw=meta_raw,
        google_raw=google_raw,
        errors=errors or None,
    )
    response["metadata"] = {
        "date_start": date_start,
        "date_end": date_end,
        "aggregation": aggregation,
        "source_row_count": len(all_rows),
    }

    return json.dumps(response, indent=2)
