import re
from datetime import datetime
from typing import Any

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class InvalidDateError(ValueError):
    pass


def validate_date(date_str: str) -> str:
    if not isinstance(date_str, str) or not _DATE_RE.match(date_str):
        raise InvalidDateError(f"Invalid date '{date_str}': expected YYYY-MM-DD")
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except (ValueError, TypeError) as exc:
        raise InvalidDateError(f"Invalid date '{date_str}': expected YYYY-MM-DD") from exc
    return date_str


def meta_spend_to_micros(spend_str: str | float) -> int:
    return int(float(spend_str) * 1_000_000)


def google_micros_to_micros(cost_micros: int | str) -> int:
    return int(cost_micros)


def micros_to_display(micros: int) -> str:
    return f"{micros / 1_000_000:.2f}"


def safe_divide(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0


def compute_derived_metrics(
    impressions: int,
    clicks: int,
    spend_micros: int,
    conversions: float,
    conversion_value: float = 0.0,
) -> dict[str, Any]:
    spend = spend_micros / 1_000_000 if spend_micros else 0.0
    return {
        "ctr": round(safe_divide(clicks, impressions) * 100, 2),
        "cpc_micros": int(safe_divide(spend_micros, clicks)),
        "cpm_micros": int(safe_divide(spend_micros, impressions) * 1000),
        "cvr": round(safe_divide(conversions, clicks) * 100, 2),
        "cost_per_conversion_micros": int(safe_divide(spend_micros, conversions)) if conversions else 0,
        "roas": round(safe_divide(conversion_value, spend), 2),
    }


def normalize_meta_insights(data: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in data.get("data", []):
        impressions = int(item.get("impressions", 0))
        clicks = int(item.get("clicks", 0))
        spend_micros = meta_spend_to_micros(item.get("spend", "0"))

        # Dedup purchase-type conversions: omni_purchase is Meta's superset of
        # purchase — summing both inflates conversions.  Use priority order and
        # pick the first match; add lead/complete_registration separately.
        conversions = 0.0
        _actions_by_type: dict[str, float] = {}
        for action in item.get("actions", []):
            if not isinstance(action, dict):
                continue
            atype = action.get("action_type")
            if atype:
                _actions_by_type[atype] = float(action.get("value", 0) or 0)
        for _ptype in ("omni_purchase", "purchase"):
            if _ptype in _actions_by_type:
                conversions += _actions_by_type[_ptype]
                break
        for _otype in ("lead", "complete_registration"):
            conversions += _actions_by_type.get(_otype, 0)

        # Extract conversion value from action_values (monetary amounts).
        # purchase, omni_purchase, and offsite_conversion.fb_pixel_purchase
        # are often duplicates of the same value — use priority order to
        # avoid double-counting.
        conversion_value = 0.0
        _av_by_type = {
            av.get("action_type"): float(av.get("value", 0) or 0)
            for av in item.get("action_values", [])
            if isinstance(av, dict)
        }
        for _atype in (
            "omni_purchase",
            "purchase",
            "offsite_conversion.fb_pixel_purchase",
        ):
            _cv = _av_by_type.get(_atype, 0)
            if _cv:
                conversion_value = _cv
                break

        derived = compute_derived_metrics(impressions, clicks, spend_micros, conversions, conversion_value)
        rows.append(
            {
                "platform": "meta",
                "account_id": item.get("account_id", ""),
                "account_name": item.get("account_name", ""),
                "campaign_id": item.get("campaign_id", ""),
                "campaign_name": item.get("campaign_name", ""),
                "adset_id": item.get("adset_id", ""),
                "adset_name": item.get("adset_name", ""),
                "ad_id": item.get("ad_id", ""),
                "ad_name": item.get("ad_name", ""),
                "date_start": item.get("date_start", ""),
                "date_stop": item.get("date_stop", ""),
                "impressions": impressions,
                "clicks": clicks,
                "spend_micros": spend_micros,
                "spend": micros_to_display(spend_micros),
                "conversions": conversions,
                "conversion_value": conversion_value,
                **derived,
            }
        )
    return rows


def normalize_google_insights(data: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in data.get("data", []):
        impressions = int(item.get("metrics.impressions", 0))
        clicks = int(item.get("metrics.clicks", 0))
        spend_micros = google_micros_to_micros(item.get("metrics.cost_micros", 0))
        conversions = float(item.get("metrics.conversions", 0))

        conversion_value = float(item.get("metrics.conversions_value", 0) or 0)
        derived = compute_derived_metrics(impressions, clicks, spend_micros, conversions, conversion_value)
        rows.append(
            {
                "platform": "google",
                "account_id": item.get("customer.id", ""),
                "account_name": item.get("customer.descriptive_name", ""),
                "campaign_id": item.get("campaign.id", ""),
                "campaign_name": item.get("campaign.name", ""),
                "date_start": item.get("segments.date", ""),
                "date_stop": item.get("segments.date", ""),
                "impressions": impressions,
                "clicks": clicks,
                "spend_micros": spend_micros,
                "spend": micros_to_display(spend_micros),
                "conversions": conversions,
                "conversion_value": conversion_value,
                **derived,
            }
        )
    return rows


_DATA_KEYS = ("data", "events", "recommendations", "accounts")


def _extract_rows_from_account(account_data: dict[str, Any]) -> list[Any]:
    for key in _DATA_KEYS:
        candidate = account_data.get(key)
        if isinstance(candidate, list) and candidate:
            return candidate

    nested_rows: list[Any] = []
    for v in account_data.values():
        if isinstance(v, dict):
            for key in _DATA_KEYS:
                candidate = v.get(key)
                if isinstance(candidate, list):
                    nested_rows.extend(candidate)
    return nested_rows


def build_diagnostics(
    meta_raw: dict[str, Any] | None = None,
    google_raw: dict[str, Any] | None = None,
) -> dict[str, Any]:
    diag: dict[str, Any] = {}
    for platform, raw in [("meta", meta_raw), ("google", google_raw)]:
        if raw is None:
            continue
        if not isinstance(raw, dict):
            diag[platform] = {"note": "unexpected raw type", "type": type(raw).__name__}
            continue
        accounts = raw.get("accounts", {})
        if not isinstance(accounts, dict):
            diag[platform] = {"note": "non-standard raw shape", "keys": sorted(raw.keys())}
            continue

        total_rows = 0
        fields_sample: set[str] = set()
        error_count = 0

        for account_data in accounts.values():
            if not isinstance(account_data, dict):
                continue
            if "error" in account_data:
                error_count += 1
                continue
            has_nested_error = any(
                isinstance(v, dict) and "error" in v
                for v in account_data.values()
            )
            if has_nested_error:
                error_count += 1
            rows = _extract_rows_from_account(account_data)
            total_rows += len(rows)
            if rows and isinstance(rows[0], dict):
                fields_sample.update(rows[0].keys())

        diag[platform] = {
            "accounts_queried": len(accounts),
            "rows_returned": total_rows,
            "errors": error_count,
            "fields_present": sorted(fields_sample),
        }
    return diag


def attach_diagnostics(
    result: dict[str, Any],
    meta_raw: dict[str, Any] | None = None,
    google_raw: dict[str, Any] | None = None,
    include_raw: bool = False,
) -> None:
    """Attach diagnostics (always) and platform_results (opt-in) to a tool response."""
    result["diagnostics"] = build_diagnostics(meta_raw, google_raw)
    if include_raw:
        platform_results: dict[str, Any] = {}
        if meta_raw is not None:
            platform_results["meta"] = meta_raw
        if google_raw is not None:
            platform_results["google"] = google_raw
        if platform_results:
            result["platform_results"] = platform_results


def build_response(
    status: str,
    rows: list[dict[str, Any]],
    errors: list[dict[str, Any]] | None = None,
    meta_raw: dict[str, Any] | None = None,
    google_raw: dict[str, Any] | None = None,
    include_raw: bool = False,
) -> dict[str, Any]:
    response: dict[str, Any] = {
        "status": status,
        "normalized_unit": "micros (1,000,000 = 1 currency unit)",
        "rows": rows,
    }
    if errors:
        response["errors"] = errors
    attach_diagnostics(response, meta_raw, google_raw, include_raw)
    return response
