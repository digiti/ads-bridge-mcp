import json
from typing import Any

from .. import mcp
from ..client import call_both
from ..normalize import attach_diagnostics


@mcp.tool()
async def compare_accounts(
    include_raw: bool = False,
) -> str:
    """List accessible Meta and Google Ads accounts for the current credentials.

    Use when: You need account IDs before running analysis tools, want to confirm
    cross-platform access, or need a quick inventory of available ad accounts.

    Args:
        None.
    """
    meta_result, google_result = await call_both(
        "get_ad_accounts",
        {},
        "list_accessible_accounts",
        {},
    )

    accounts: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    if "error" in meta_result:
        errors.append({"platform": "meta", "error": str(meta_result["error"])})
    else:
        for acc in meta_result.get("data", []):
            accounts.append(
                {
                    "platform": "meta",
                    "id": acc.get("id", ""),
                    "name": acc.get("name", "Unknown"),
                    "status": acc.get("account_status", ""),
                    "currency": acc.get("currency", ""),
                }
            )

    if "error" in google_result:
        errors.append({"platform": "google", "error": str(google_result["error"])})
    else:
        for acc in google_result.get("accounts", []):
            accounts.append(
                {
                    "platform": "google",
                    "id": acc.get("id", ""),
                    "name": acc.get("name", "Unknown"),
                    "is_manager": acc.get("is_manager", False),
                    "access_type": acc.get("access_type", ""),
                    "level": acc.get("level", 0),
                }
            )

    status = "ok" if not errors else ("partial" if accounts else "error")
    result = {
        "status": status,
        "accounts": accounts,
        "total": len(accounts),
        "by_platform": {
            "meta": len([a for a in accounts if a["platform"] == "meta"]),
            "google": len([a for a in accounts if a["platform"] == "google"]),
        },
    }
    if errors:
        result["errors"] = errors

    meta_raw = {"accounts": {"all": meta_result}}
    google_raw = {"accounts": {"all": google_result}}
    attach_diagnostics(result, meta_raw, google_raw, include_raw)

    return json.dumps(result, indent=2)
