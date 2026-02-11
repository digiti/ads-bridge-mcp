import os

from . import mcp


from .tools import accounts, anomalies, budget, campaigns, performance, summary  # noqa: E402,F401


def main() -> None:
    port = int(os.environ.get("BRIDGE_PORT", "8080"))
    mcp.run(transport="streamable-http", host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
