import asyncio
import json
from datetime import datetime, timezone
from typing import Any

from .. import mcp
from ..client import call_google_tool, call_meta_tool
from ..normalize import InvalidDateError, attach_diagnostics, validate_date


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None

    text = str(value).strip()
    if not text:
        return None

    iso_text = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso_text)
    except ValueError:
        return None

    # Ensure all returned datetimes are UTC-aware so sorting never
    # mixes offset-aware and offset-naive objects.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _normalize_meta_event(item: dict[str, Any], account_id: str) -> dict[str, Any]:
    event_time = item.get("event_time") or item.get("created_time") or item.get("timestamp") or ""
    actor_name = item.get("actor_name") or item.get("actor") or item.get("user_name") or ""
    action = item.get("translated_event_type") or item.get("event_type") or item.get("action") or "unknown"
    object_type = item.get("object_type") or item.get("entity_type") or ""
    object_name = item.get("object_name") or item.get("entity_name") or item.get("object_id") or ""
    details = item.get("extra_data") or item.get("details") or None

    return {
        "platform": "meta",
        "timestamp": str(event_time),
        "actor": str(actor_name),
        "action": str(action),
        "object_type": str(object_type),
        "object_name": str(object_name),
        "details": details,
        "account_id": account_id,
    }


def _normalize_google_event(item: dict[str, Any], account_id: str) -> dict[str, Any]:
    timestamp = item.get("change_date_time") or item.get("change_event.change_date_time") or item.get("timestamp") or ""
    actor = item.get("user_email") or item.get("change_event.user_email") or item.get("user") or ""
    action = (
        item.get("resource_change_operation")
        or item.get("change_event.resource_change_operation")
        or item.get("operation")
        or "unknown"
    )
    object_type = item.get("change_resource_type") or item.get("change_event.change_resource_type") or item.get("resource_type") or ""
    object_name = item.get("change_resource_name") or item.get("change_event.change_resource_name") or item.get("resource_name") or ""

    return {
        "platform": "google",
        "timestamp": str(timestamp),
        "actor": str(actor),
        "action": str(action),
        "object_type": str(object_type),
        "object_name": str(object_name),
        "details": None,
        "account_id": account_id,
    }


@mcp.tool()
async def get_change_log(
    meta_account_ids: list[str],
    google_account_ids: list[str],
    date_start: str,
    date_end: str,
    google_login_customer_id: str | None = None,
    limit: int = 50,
    include_raw: bool = False,
) -> str:
    """Fetch and unify account change history from Meta and Google Ads.

    Use when: You need an audit-style timeline of edits (who changed what and
    when) across ad platforms for troubleshooting, governance, or handoff review.

    Args:
        meta_account_ids: Meta ad account IDs to pull activity logs from.
        google_account_ids: Google Ads customer IDs to pull change events from.
        date_start: Inclusive start date for the audit window in YYYY-MM-DD format.
        date_end: Inclusive end date for the audit window in YYYY-MM-DD format.
        google_login_customer_id: Optional manager account ID for Google Ads API access.
        limit: Maximum events to request per account per platform.
    """
    try:
        validate_date(date_start)
        validate_date(date_end)
    except InvalidDateError as exc:
        result: dict[str, Any] = {"status": "error", "events": [], "errors": [{"source": "validation", "error": str(exc)}]}
        attach_diagnostics(result)
        return json.dumps(result, indent=2)

    if date_start > date_end:
        result = {"status": "error", "events": [], "errors": [{"source": "validation", "error": f"date_start '{date_start}' is after date_end '{date_end}'"}]}
        attach_diagnostics(result)
        return json.dumps(result, indent=2)

    errors: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    meta_raw: dict[str, Any] = {"accounts": {}}
    google_raw: dict[str, Any] = {"accounts": {}}

    effective_limit = max(int(limit), 0)

    meta_tasks = [
        call_meta_tool(
            "get_account_activities",
            {
                "account_id": account_id,
                "since": date_start,
                "until": date_end,
                "limit": effective_limit,
            },
        )
        for account_id in meta_account_ids
    ]

    google_tasks = [
        call_google_tool(
            "get_change_events",
            {
                "customer_id": account_id,
                "start_date": date_start,
                "end_date": date_end,
                "limit": effective_limit,
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
            if isinstance(item, dict):
                events.append(_normalize_meta_event(item, account_id))

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

        raw_events = result.get("events") or result.get("data") or []
        for item in raw_events:
            if isinstance(item, dict):
                events.append(_normalize_google_event(item, account_id))

    events.sort(
        key=lambda event: (
            _parse_timestamp(event.get("timestamp")) is not None,
            _parse_timestamp(event.get("timestamp")) or datetime.min,
            str(event.get("timestamp", "")),
        ),
        reverse=True,
    )

    meta_count = sum(1 for event in events if event.get("platform") == "meta")
    google_count = sum(1 for event in events if event.get("platform") == "google")

    status = "ok" if not errors else ("partial" if events else "error")
    result: dict[str, Any] = {
        "status": status,
        "date_start": date_start,
        "date_end": date_end,
        "events": events,
        "count": len(events),
        "by_platform": {"meta": meta_count, "google": google_count},
    }
    if errors:
        result["errors"] = errors

    attach_diagnostics(result, meta_raw, google_raw, include_raw)

    return json.dumps(result, indent=2)
