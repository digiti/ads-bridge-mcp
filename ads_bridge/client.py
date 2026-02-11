import asyncio
import importlib
import json
import os
from typing import Any


META_MCP_URL = os.environ.get("META_MCP_URL", "http://meta-ads-mcp:8080/mcp")
GOOGLE_MCP_URL = os.environ.get("GOOGLE_MCP_URL", "http://google-ads-mcp:8080/mcp")


def _get_client_class() -> Any:
    fastmcp_module = importlib.import_module("fastmcp")
    return getattr(fastmcp_module, "Client")


def _extract_result_payload(result: Any) -> dict[str, Any]:
    content = getattr(result, "content", None)

    if not content:
        return {}

    first = content[0]
    text = getattr(first, "text", None)

    if text is None:
        if isinstance(first, dict):
            text = first.get("text")
        elif isinstance(first, str):
            text = first

    if text is None:
        return {"raw_content": str(content)}

    if isinstance(text, dict):
        return text

    if not isinstance(text, str):
        return {"raw_text": str(text)}

    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
        return {"data": parsed}
    except json.JSONDecodeError:
        return {"raw_text": text}


async def call_meta_tool(tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    client_cls = _get_client_class()
    async with client_cls(META_MCP_URL) as client:
        result = await client.call_tool(tool_name, arguments)
        if getattr(result, "is_error", False):
            return {"error": str(getattr(result, "content", "Unknown Meta MCP error")), "platform": "meta"}
        return _extract_result_payload(result)


async def call_google_tool(tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    client_cls = _get_client_class()
    async with client_cls(GOOGLE_MCP_URL) as client:
        result = await client.call_tool(tool_name, arguments)
        if getattr(result, "is_error", False):
            return {
                "error": str(getattr(result, "content", "Unknown Google MCP error")),
                "platform": "google",
            }
        return _extract_result_payload(result)


async def call_both(
    meta_tool: str,
    meta_args: dict[str, Any],
    google_tool: str,
    google_args: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    results = await asyncio.gather(
        call_meta_tool(meta_tool, meta_args),
        call_google_tool(google_tool, google_args),
        return_exceptions=True,
    )
    meta_result: dict[str, Any] | BaseException = results[0]
    google_result: dict[str, Any] | BaseException = results[1]

    if isinstance(meta_result, BaseException):
        meta_result = {"error": str(meta_result), "platform": "meta"}
    if isinstance(google_result, BaseException):
        google_result = {"error": str(google_result), "platform": "google"}

    return meta_result, google_result
