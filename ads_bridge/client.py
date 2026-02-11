import asyncio
import importlib
import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

META_MCP_URL = os.environ.get("META_MCP_URL", "http://meta-ads-mcp:8080/mcp")
GOOGLE_MCP_URL = os.environ.get("GOOGLE_MCP_URL", "http://google-ads-mcp:8080/mcp")

MAX_RETRIES = int(os.environ.get("BRIDGE_MAX_RETRIES", "3"))
RETRY_BASE_DELAY = float(os.environ.get("BRIDGE_RETRY_BASE_DELAY", "0.5"))

_meta_client: Any = None
_google_client: Any = None
_meta_lock = asyncio.Lock()
_google_lock = asyncio.Lock()


def _get_client_class() -> Any:
    fastmcp_module = importlib.import_module("fastmcp")
    return getattr(fastmcp_module, "Client")


async def _get_meta_client() -> Any:
    global _meta_client
    async with _meta_lock:
        if _meta_client is None:
            client_cls = _get_client_class()
            _meta_client = client_cls(META_MCP_URL)
            await _meta_client.__aenter__()
        return _meta_client


async def _get_google_client() -> Any:
    global _google_client
    async with _google_lock:
        if _google_client is None:
            client_cls = _get_client_class()
            _google_client = client_cls(GOOGLE_MCP_URL)
            await _google_client.__aenter__()
        return _google_client


async def _reset_meta_client() -> None:
    global _meta_client
    async with _meta_lock:
        if _meta_client is not None:
            try:
                await _meta_client.__aexit__(None, None, None)
            except Exception:
                pass
            _meta_client = None


async def _reset_google_client() -> None:
    global _google_client
    async with _google_lock:
        if _google_client is not None:
            try:
                await _google_client.__aexit__(None, None, None)
            except Exception:
                pass
            _google_client = None


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


async def _call_with_retry(
    get_client_fn: Any,
    reset_client_fn: Any,
    tool_name: str,
    arguments: dict[str, Any],
    platform: str,
) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            client = await get_client_fn()
            result = await client.call_tool(tool_name, arguments)
            if getattr(result, "is_error", False):
                return {"error": str(getattr(result, "content", f"Unknown {platform} MCP error")), "platform": platform}
            return _extract_result_payload(result)
        except Exception as exc:
            last_error = exc
            logger.warning("Attempt %d/%d failed for %s.%s: %s", attempt + 1, MAX_RETRIES, platform, tool_name, exc)
            await reset_client_fn()
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_BASE_DELAY * (2 ** attempt)
                await asyncio.sleep(delay)

    return {"error": str(last_error), "platform": platform}


async def call_meta_tool(tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    return await _call_with_retry(_get_meta_client, _reset_meta_client, tool_name, arguments, "meta")


async def call_google_tool(tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    return await _call_with_retry(_get_google_client, _reset_google_client, tool_name, arguments, "google")


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
