from typing import Any


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

        conversions = 0.0
        for action in item.get("actions", []):
            if action.get("action_type") in (
                "purchase",
                "lead",
                "complete_registration",
                "omni_purchase",
            ):
                conversions += float(action.get("value", 0))

        # Extract conversion value from action_values (monetary amounts).
        # purchase, omni_purchase, and offsite_conversion.fb_pixel_purchase
        # are often duplicates of the same value — use priority order to
        # avoid double-counting.
        conversion_value = 0.0
        _av_by_type = {
            av.get("action_type"): float(av.get("value", 0))
            for av in item.get("action_values", [])
        }
        for _atype in (
            "purchase",
            "omni_purchase",
            "offsite_conversion.fb_pixel_purchase",
        ):
            if _atype in _av_by_type:
                conversion_value = _av_by_type[_atype]
                break

        derived = compute_derived_metrics(impressions, clicks, spend_micros, conversions, conversion_value)
        rows.append(
            {
                "platform": "meta",
                "account_id": item.get("account_id", ""),
                "account_name": item.get("account_name", ""),
                "campaign_id": item.get("campaign_id", ""),
                "campaign_name": item.get("campaign_name", ""),
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


def build_response(
    status: str,
    rows: list[dict[str, Any]],
    errors: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    response: dict[str, Any] = {
        "status": status,
        "normalized_unit": "micros (1,000,000 = 1 currency unit)",
        "rows": rows,
    }
    if errors:
        response["errors"] = errors
    return response
