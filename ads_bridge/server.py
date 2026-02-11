import os

from . import mcp


from .tools import (  # noqa: E402,F401
    accounts,
    ad_performance,
    anomalies,
    budget,
    campaigns,
    changelog,
    creative_analysis,
    daily_trends,
    demographics,
    devices,
    geo,
    optimization,
    pacing,
    performance,
    period_comparison,
    placements,
    summary,
)


def main() -> None:
    port = int(os.environ.get("BRIDGE_PORT", "8080"))
    mcp.run(transport="streamable-http", host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
