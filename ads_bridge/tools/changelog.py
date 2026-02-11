import asyncio
import json
from datetime import datetime
from typing import Any

from .. import mcp
from ..client import call_google_tool, call_meta_tool


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None

    text = str(value).strip()
    if not text:
        return None

    iso_text = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(iso_text)
    except ValueError:
        return None


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
) -> str:
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

        for item in result.get("data", []):
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
        "platform_results": {"meta": meta_raw, "google": google_raw},
    }
    if errors:
        result["errors"] = errors

    return json.dumps(result, indent=2)
