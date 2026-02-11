import importlib


fastmcp_module = importlib.import_module("fastmcp")
FastMCP = getattr(fastmcp_module, "FastMCP")

mcp = FastMCP(
    "Ads Bridge",
    description="Cross-platform advertising bridge - unified analytics across Meta Ads and Google Ads",
)
