import atexit
import os

from . import mcp
from .client import shutdown_clients


from .tools import (  # noqa: E402,F401
    accounts,
    ad_performance,
    anomalies,
    breakdown,
    budget,
    changelog,
    creative_analysis,
    daily_trends,
    optimization,
    performance,
    period_comparison,
)


def _sync_shutdown() -> None:
    import asyncio

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(shutdown_clients())
        else:
            loop.run_until_complete(shutdown_clients())
    except RuntimeError:
        pass


atexit.register(_sync_shutdown)


def main() -> None:
    try:
        port = int(os.environ.get("BRIDGE_PORT", "8080"))
    except (ValueError, TypeError):
        port = 8080
    mcp.run(transport="streamable-http", host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
