import asyncio
import calendar
import json
from datetime import UTC, datetime, timedelta
from typing import Any

from .. import mcp
from ..client import call_google_tool, call_meta_tool
from ..normalize import attach_diagnostics, micros_to_display, normalize_google_insights, normalize_meta_insights, safe_divide


def _platform_totals(rows: list[dict[str, Any]]) -> dict[str, float]:
    spend_micros = float(sum(int(row.get("spend_micros", 0)) for row in rows))
    conversion_value = float(sum(float(row.get("conversion_value", 0)) for row in rows))
    roas = safe_divide(conversion_value, spend_micros / 1_000_000) if spend_micros else 0.0
    return {
        "spend_micros": spend_micros,
        "conversion_value": conversion_value,
        "roas": roas,
    }


def _build_recommendation(meta_roas: float, google_roas: float) -> str:
    if meta_roas <= 0 and google_roas <= 0:
        return "ROAS data is limited; keep current allocation until conversion value tracking improves."
    if meta_roas > google_roas * 1.2:
        return "Meta shows materially stronger ROAS; consider reallocating incremental budget toward Meta campaigns."
    if google_roas > meta_roas * 1.2:
        return "Google shows materially stronger ROAS; consider reallocating incremental budget toward Google campaigns."
    return "ROAS is relatively balanced; maintain current split and optimize within each platform."


def _is_active_meta_campaign(campaign: dict[str, Any]) -> bool:
    status = str(campaign.get("status", campaign.get("effective_status", ""))).upper()
    if not status:
        return True
    return status in {"ACTIVE", "ENABLED"}


def _meta_monthly_budget_micros(campaigns: list[dict[str, Any]], total_days_in_month: int) -> int:
    budget_micros = 0
    for campaign in campaigns:
        if not isinstance(campaign, dict):
            continue
        if not _is_active_meta_campaign(campaign):
            continue

        daily_budget = campaign.get("daily_budget")
        lifetime_budget = campaign.get("lifetime_budget")

        if daily_budget not in (None, ""):
            budget_micros += int(daily_budget) * 10_000 * total_days_in_month
            continue
        if lifetime_budget not in (None, ""):
            budget_micros += int(lifetime_budget) * 10_000

    return budget_micros


def _google_monthly_budget_micros(rows: list[dict[str, Any]], total_days_in_month: int) -> int:
    budget_micros = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        amount_micros = int(row.get("campaign_budget.amount_micros", 0) or 0)
        total_amount_micros = int(row.get("campaign_budget.total_amount_micros", 0) or 0)
        if amount_micros > 0:
            budget_micros += amount_micros * total_days_in_month
        elif total_amount_micros > 0:
            budget_micros += total_amount_micros
    return budget_micros


def _google_spend_micros(rows: list[dict[str, Any]]) -> int:
    return sum(int(row.get("metrics.cost_micros", 0) or 0) for row in rows if isinstance(row, dict))


@mcp.tool()
async def get_budget_analysis(
    meta_account_ids: list[str],
    google_account_ids: list[str],
    analysis_type: str = "allocation",
    date_start: str | None = None,
    date_end: str | None = None,
    google_login_customer_id: str | None = None,
    month_start: str | None = None,
    month_end: str | None = None,
    include_raw: bool = False,
) -> str:
    """Analyze cross-platform budget by mode: use allocation for spend split and ROAS over a custom date range, or pacing for in-month budget pacing and projected monthly spend."""
    if analysis_type not in {"allocation", "pacing"}:
        return json.dumps(
            {
                "status": "error",
                "error": "Invalid analysis_type. Supported values are: allocation, pacing.",
            },
            indent=2,
        )

    if analysis_type == "allocation":
        if not date_start or not date_end:
            return json.dumps(
                {
                    "status": "error",
                    "error": "date_start and date_end are required when analysis_type is 'allocation'.",
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

        meta_totals = _platform_totals(meta_rows)
        google_totals = _platform_totals(google_rows)

        total_spend_micros = int(meta_totals["spend_micros"] + google_totals["spend_micros"])
        meta_spend_micros = int(meta_totals["spend_micros"])
        google_spend_micros = int(google_totals["spend_micros"])

        meta_pct = round((meta_spend_micros / total_spend_micros) * 100, 2) if total_spend_micros else 0.0
        google_pct = round((google_spend_micros / total_spend_micros) * 100, 2) if total_spend_micros else 0.0

        result: dict[str, Any] = {
            "status": "ok" if not errors else ("partial" if total_spend_micros > 0 else "error"),
            "date_start": date_start,
            "date_end": date_end,
            "spend_allocation": {
                "meta": {
                    "spend_micros": meta_spend_micros,
                    "spend": micros_to_display(meta_spend_micros),
                    "pct": meta_pct,
                    "roas": round(meta_totals["roas"], 4),
                },
                "google": {
                    "spend_micros": google_spend_micros,
                    "spend": micros_to_display(google_spend_micros),
                    "pct": google_pct,
                    "roas": round(google_totals["roas"], 4),
                },
                "total_spend_micros": total_spend_micros,
                "total_spend": micros_to_display(total_spend_micros),
            },
            "recommendation": _build_recommendation(meta_totals["roas"], google_totals["roas"]),
        }
        if errors:
            result["errors"] = errors

        attach_diagnostics(result, meta_raw, google_raw, include_raw)

        return json.dumps(result, indent=2)

    now = datetime.now(UTC).date()
    if month_start:
        start_date = datetime.strptime(month_start, "%Y-%m-%d").date()
    else:
        start_date = now.replace(day=1)

    if month_end:
        end_date = datetime.strptime(month_end, "%Y-%m-%d").date()
    else:
        last_day = calendar.monthrange(start_date.year, start_date.month)[1]
        end_date = start_date.replace(day=last_day)

    total_days_in_month = (end_date - start_date).days + 1
    today_in_window = min(max(now, start_date), end_date)
    days_elapsed = (today_in_window - start_date + timedelta(days=1)).days
    days_remaining = max(total_days_in_month - days_elapsed, 0)
    today_str = today_in_window.isoformat()

    errors: list[dict[str, Any]] = []
    accounts: list[dict[str, Any]] = []
    meta_raw: dict[str, Any] = {"campaigns": {}, "insights": {}}
    google_raw: dict[str, Any] = {"budgets": {}, "spend": {}}

    meta_campaign_tasks = [
        call_meta_tool("get_campaigns", {"account_id": account_id, "limit": 100}) for account_id in meta_account_ids
    ]
    meta_insight_tasks = [
        call_meta_tool(
            "get_insights",
            {
                "account_id": account_id,
                "time_range": {"since": start_date.isoformat(), "until": today_str},
                "level": "account",
            },
        )
        for account_id in meta_account_ids
    ]

    google_budget_fields = [
        "campaign_budget.amount_micros",
        "campaign_budget.total_amount_micros",
        "campaign.id",
        "campaign.name",
        "campaign.status",
    ]
    google_budget_tasks = [
        call_google_tool(
            "search_ads",
            {
                "customer_id": account_id,
                "resource": "campaign_budget",
                "fields": google_budget_fields,
                "conditions": ["campaign.status = 'ENABLED'"],
                **({"login_customer_id": google_login_customer_id} if google_login_customer_id else {}),
            },
        )
        for account_id in google_account_ids
    ]

    google_spend_fields = [
        "customer.id",
        "customer.descriptive_name",
        "metrics.cost_micros",
        "segments.date",
    ]
    google_spend_tasks = [
        call_google_tool(
            "search_ads",
            {
                "customer_id": account_id,
                "resource": "customer",
                "fields": google_spend_fields,
                "conditions": [f"segments.date BETWEEN '{start_date.isoformat()}' AND '{today_str}'"],
                **({"login_customer_id": google_login_customer_id} if google_login_customer_id else {}),
            },
        )
        for account_id in google_account_ids
    ]

    meta_campaign_results = await asyncio.gather(*meta_campaign_tasks, return_exceptions=True)
    meta_insight_results = await asyncio.gather(*meta_insight_tasks, return_exceptions=True)
    google_budget_results = await asyncio.gather(*google_budget_tasks, return_exceptions=True)
    google_spend_results = await asyncio.gather(*google_spend_tasks, return_exceptions=True)

    for idx, account_id in enumerate(meta_account_ids):
        campaign_raw = meta_campaign_results[idx]
        insight_raw = meta_insight_results[idx]

        account_errors = False

        if isinstance(campaign_raw, BaseException):
            message = str(campaign_raw)
            errors.append({"platform": "meta", "account_id": account_id, "error": message})
            meta_raw["campaigns"][account_id] = {"error": message}
            account_errors = True
        elif not isinstance(campaign_raw, dict):
            message = f"Unexpected Meta response type: {type(campaign_raw).__name__}"
            errors.append({"platform": "meta", "account_id": account_id, "error": message})
            meta_raw["campaigns"][account_id] = {"error": message}
            account_errors = True
        else:
            meta_raw["campaigns"][account_id] = campaign_raw
            if "error" in campaign_raw:
                errors.append({"platform": "meta", "account_id": account_id, "error": str(campaign_raw["error"])})
                account_errors = True

        if isinstance(insight_raw, BaseException):
            message = str(insight_raw)
            errors.append({"platform": "meta", "account_id": account_id, "error": message})
            meta_raw["insights"][account_id] = {"error": message}
            account_errors = True
        elif not isinstance(insight_raw, dict):
            message = f"Unexpected Meta response type: {type(insight_raw).__name__}"
            errors.append({"platform": "meta", "account_id": account_id, "error": message})
            meta_raw["insights"][account_id] = {"error": message}
            account_errors = True
        else:
            meta_raw["insights"][account_id] = insight_raw
            if "error" in insight_raw:
                errors.append({"platform": "meta", "account_id": account_id, "error": str(insight_raw["error"])})
                account_errors = True

        if account_errors:
            continue

        campaigns = meta_raw["campaigns"][account_id].get("data", [])
        insights = meta_raw["insights"][account_id].get("data", [])
        budget_micros = _meta_monthly_budget_micros(campaigns, total_days_in_month)
        spent_micros = sum(int(float(row.get("spend", 0) or 0) * 1_000_000) for row in insights if isinstance(row, dict))
        account_name = str(insights[0].get("account_name", "")) if insights else ""

        daily_avg_spend_micros = int(safe_divide(float(spent_micros), float(days_elapsed)))
        projected_spend_micros = daily_avg_spend_micros * total_days_in_month
        expected_to_date = safe_divide(float(budget_micros * days_elapsed), float(total_days_in_month))
        pacing_pct = round(safe_divide(float(spent_micros), expected_to_date) * 100, 2) if expected_to_date else 0.0
        status = "on_track" if 85 <= pacing_pct <= 115 else ("underspending" if pacing_pct < 85 else "overspending")

        accounts.append(
            {
                "platform": "meta",
                "account_id": account_id,
                "account_name": account_name,
                "budget_micros": budget_micros,
                "budget": micros_to_display(budget_micros),
                "spent_micros": spent_micros,
                "spent": micros_to_display(spent_micros),
                "projected_spend_micros": projected_spend_micros,
                "projected_spend": micros_to_display(projected_spend_micros),
                "pacing_pct": pacing_pct,
                "status": status,
            }
        )

    for idx, account_id in enumerate(google_account_ids):
        budget_raw = google_budget_results[idx]
        spend_raw = google_spend_results[idx]

        account_errors = False

        if isinstance(budget_raw, BaseException):
            message = str(budget_raw)
            errors.append({"platform": "google", "account_id": account_id, "error": message})
            google_raw["budgets"][account_id] = {"error": message}
            account_errors = True
        elif not isinstance(budget_raw, dict):
            message = f"Unexpected Google response type: {type(budget_raw).__name__}"
            errors.append({"platform": "google", "account_id": account_id, "error": message})
            google_raw["budgets"][account_id] = {"error": message}
            account_errors = True
        else:
            google_raw["budgets"][account_id] = budget_raw
            if "error" in budget_raw:
                errors.append({"platform": "google", "account_id": account_id, "error": str(budget_raw["error"])})
                account_errors = True

        if isinstance(spend_raw, BaseException):
            message = str(spend_raw)
            errors.append({"platform": "google", "account_id": account_id, "error": message})
            google_raw["spend"][account_id] = {"error": message}
            account_errors = True
        elif not isinstance(spend_raw, dict):
            message = f"Unexpected Google response type: {type(spend_raw).__name__}"
            errors.append({"platform": "google", "account_id": account_id, "error": message})
            google_raw["spend"][account_id] = {"error": message}
            account_errors = True
        else:
            google_raw["spend"][account_id] = spend_raw
            if "error" in spend_raw:
                errors.append({"platform": "google", "account_id": account_id, "error": str(spend_raw["error"])})
                account_errors = True

        if account_errors:
            continue

        budget_rows = google_raw["budgets"][account_id].get("data", [])
        spend_rows = google_raw["spend"][account_id].get("data", [])
        budget_micros = _google_monthly_budget_micros(budget_rows, total_days_in_month)
        spent_micros = _google_spend_micros(spend_rows)
        account_name = str(spend_rows[0].get("customer.descriptive_name", "")) if spend_rows else ""

        daily_avg_spend_micros = int(safe_divide(float(spent_micros), float(days_elapsed)))
        projected_spend_micros = daily_avg_spend_micros * total_days_in_month
        expected_to_date = safe_divide(float(budget_micros * days_elapsed), float(total_days_in_month))
        pacing_pct = round(safe_divide(float(spent_micros), expected_to_date) * 100, 2) if expected_to_date else 0.0
        status = "on_track" if 85 <= pacing_pct <= 115 else ("underspending" if pacing_pct < 85 else "overspending")

        accounts.append(
            {
                "platform": "google",
                "account_id": account_id,
                "account_name": account_name,
                "budget_micros": budget_micros,
                "budget": micros_to_display(budget_micros),
                "spent_micros": spent_micros,
                "spent": micros_to_display(spent_micros),
                "projected_spend_micros": projected_spend_micros,
                "projected_spend": micros_to_display(projected_spend_micros),
                "pacing_pct": pacing_pct,
                "status": status,
            }
        )

    meta_accounts = [row for row in accounts if row.get("platform") == "meta"]
    google_accounts = [row for row in accounts if row.get("platform") == "google"]

    meta_total_budget = sum(int(row.get("budget_micros", 0)) for row in meta_accounts)
    meta_total_spent = sum(int(row.get("spent_micros", 0)) for row in meta_accounts)
    google_total_budget = sum(int(row.get("budget_micros", 0)) for row in google_accounts)
    google_total_spent = sum(int(row.get("spent_micros", 0)) for row in google_accounts)

    meta_expected_to_date = safe_divide(float(meta_total_budget * days_elapsed), float(total_days_in_month))
    google_expected_to_date = safe_divide(float(google_total_budget * days_elapsed), float(total_days_in_month))

    result: dict[str, Any] = {
        "status": "ok" if not errors else ("partial" if accounts else "error"),
        "month": start_date.strftime("%Y-%m"),
        "days_elapsed": days_elapsed,
        "days_remaining": days_remaining,
        "total_days": total_days_in_month,
        "accounts": accounts,
        "summary": {
            "meta": {
                "total_budget_micros": meta_total_budget,
                "total_spent_micros": meta_total_spent,
                "overall_pacing_pct": round(safe_divide(float(meta_total_spent), meta_expected_to_date) * 100, 2)
                if meta_expected_to_date
                else 0.0,
            },
            "google": {
                "total_budget_micros": google_total_budget,
                "total_spent_micros": google_total_spent,
                "overall_pacing_pct": round(safe_divide(float(google_total_spent), google_expected_to_date) * 100, 2)
                if google_expected_to_date
                else 0.0,
            },
        },
    }
    if errors:
        result["errors"] = errors

    attach_diagnostics(result, meta_raw, google_raw, include_raw)

    return json.dumps(result, indent=2)
