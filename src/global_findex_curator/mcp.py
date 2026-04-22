"""MCP (Model Context Protocol) integration utilities."""

import asyncio
import json
import re
from collections.abc import Callable

from databricks.sdk import WorkspaceClient
from databricks_mcp import DatabricksMCPClient
from databricks_mcp.oauth_provider import DatabricksOAuthClientProvider
from loguru import logger
from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from pydantic import BaseModel

_GENIE_QUERY_PREFIX = "query_space_"
_GENIE_POLL_PREFIX = "poll_response_"
_GENIE_COMPLETE_STATUSES = {"COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"}
# Continuation fields pruned from the query tool schema. Our wrapper always opens a
# fresh MCP session and starts a new conversation, so exposing these only tempts
# the LLM to hallucinate values (we observed e.g. PERMISSION_DENIED on a made-up
# conversation id like "barriers_to_mobile_phone_ownership").
_GENIE_CONTINUATION_FIELDS = ("conversation_id", "message_id")
_RATE_LIMIT_RE = re.compile(r"REQUEST_LIMIT_EXCEEDED|Rate limit exceeded", re.IGNORECASE)
_RETRY_AFTER_RE = re.compile(r"Retry after (\d+)\s*second", re.IGNORECASE)


class ToolInfo(BaseModel):
    """Tool information for agent integration.

    Attributes:
        name: Tool name
        spec: JSON description of the tool (OpenAI Responses format)
        exec_fn: Function that implements the tool logic
    """

    name: str
    spec: dict
    exec_fn: Callable

    class Config:
        arbitrary_types_allowed = True


def create_managed_exec_fn(
    server_url: str, tool_name: str, w: WorkspaceClient
) -> Callable:
    """Create an execution function for an MCP tool.

    Args:
        server_url: MCP server URL
        tool_name: Name of the tool
        w: Databricks workspace client

    Returns:
        Callable that executes the tool
    """

    def exec_fn(**kwargs):
        client = DatabricksMCPClient(server_url=server_url, workspace_client=w)
        response = client.call_tool(tool_name, kwargs)
        return "".join([c.text for c in response.content])

    return exec_fn


def _flatten_exc(exc: BaseException) -> list[BaseException]:
    subs = getattr(exc, "exceptions", None)
    if not subs:
        return [exc]
    out: list[BaseException] = []
    for s in subs:
        out.extend(_flatten_exc(s))
    return out


def _rate_limit_retry_after(exc: BaseException) -> int | None:
    """Return retry-after seconds if any leaf is a Genie rate-limit error, else None."""
    for leaf in _flatten_exc(exc):
        msg = str(leaf)
        if _RATE_LIMIT_RE.search(msg):
            m = _RETRY_AFTER_RE.search(msg)
            return int(m.group(1)) if m else 60
    return None


async def _genie_query_and_poll(
    server_url: str,
    query_tool_name: str,
    poll_tool_name: str,
    w: WorkspaceClient,
    kwargs: dict,
    max_polls: int = 30,
    poll_interval: float = 2.0,
    max_rate_limit_retries: int = 2,
) -> str:
    """Query Genie and poll for completion within a single MCP session.

    Using a single session for both the initial query and all subsequent polls
    avoids the ownership error that arises when each call_tool opens a new
    HTTP connection and Genie ties conversation ownership to the originating session.

    On Genie rate-limit errors (McpError: REQUEST_LIMIT_EXCEEDED), we re-open a
    fresh session and retry up to max_rate_limit_retries times, sleeping for the
    "Retry after N seconds" hint advertised by the server.
    """
    attempts = 0
    while True:
        try:
            async with streamablehttp_client(
                url=server_url, auth=DatabricksOAuthClientProvider(w)
            ) as (read_stream, write_stream, _):
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()

                    response = await session.call_tool(query_tool_name, kwargs)
                    result_text = "".join([c.text for c in response.content])

                    try:
                        result = json.loads(result_text)
                    except (json.JSONDecodeError, ValueError):
                        return result_text

                    status = result.get("status", "COMPLETED")
                    conversation_id = result.get("conversationId") or result.get(
                        "conversation_id"
                    )
                    message_id = result.get("messageId") or result.get("message_id")

                    for _ in range(max_polls):
                        if (
                            status in _GENIE_COMPLETE_STATUSES
                            or not conversation_id
                            or not message_id
                        ):
                            break
                        await asyncio.sleep(poll_interval)
                        poll_response = await session.call_tool(
                            poll_tool_name,
                            {
                                "conversation_id": conversation_id,
                                "message_id": message_id,
                            },
                        )
                        result_text = "".join([c.text for c in poll_response.content])
                        try:
                            result = json.loads(result_text)
                        except (json.JSONDecodeError, ValueError):
                            return result_text
                        status = result.get("status", "COMPLETED")
                        conversation_id = (
                            result.get("conversationId")
                            or result.get("conversation_id")
                            or conversation_id
                        )
                        message_id = (
                            result.get("messageId")
                            or result.get("message_id")
                            or message_id
                        )

            return result_text
        except BaseException as exc:
            retry_after = _rate_limit_retry_after(exc)
            if retry_after is None or attempts >= max_rate_limit_retries:
                raise
            attempts += 1
            logger.warning(
                f"Genie rate limit hit (attempt {attempts}/{max_rate_limit_retries}); "
                f"sleeping {retry_after}s before retry."
            )
            await asyncio.sleep(retry_after)


def _create_genie_exec_fn(
    server_url: str,
    query_tool_name: str,
    poll_tool_name: str,
    w: WorkspaceClient,
) -> Callable:
    def exec_fn(**kwargs: dict) -> str:
        return asyncio.run(
            _genie_query_and_poll(server_url, query_tool_name, poll_tool_name, w, kwargs)
        )

    return exec_fn


def _prune_continuation_fields(spec: dict) -> dict:
    """Return a copy of the tool spec with Genie continuation fields removed.

    The wrapper always opens a new session and starts a fresh conversation, so
    conversation_id / message_id are inert pass-throughs for us; leaving them in
    the advertised schema only invites LLM hallucinations.
    """
    pruned = json.loads(json.dumps(spec))
    params = pruned.get("function", {}).get("parameters", {}) or {}
    props = params.get("properties")
    if isinstance(props, dict):
        for field in _GENIE_CONTINUATION_FIELDS:
            props.pop(field, None)
    required = params.get("required")
    if isinstance(required, list):
        params["required"] = [r for r in required if r not in _GENIE_CONTINUATION_FIELDS]
    return pruned


def _wrap_genie_tools(
    tools: list[ToolInfo], server_url: str, w: WorkspaceClient
) -> list[ToolInfo]:
    """Replace async Genie query/poll pairs with a single synchronous tool.

    Identifies query_space_* / poll_response_* pairs by their shared Genie space ID
    suffix, wraps the query tool with internal polling, and removes the poll tool
    from the list so the LLM never sees it.
    """
    poll_tool_names = {
        t.name[len(_GENIE_POLL_PREFIX) :]: t.name
        for t in tools
        if t.name.startswith(_GENIE_POLL_PREFIX)
    }

    wrapped = []
    for tool in tools:
        if tool.name.startswith(_GENIE_POLL_PREFIX):
            continue
        if tool.name.startswith(_GENIE_QUERY_PREFIX):
            space_id = tool.name[len(_GENIE_QUERY_PREFIX) :]
            if space_id in poll_tool_names:
                wrapped.append(
                    ToolInfo(
                        name=tool.name,
                        spec=_prune_continuation_fields(tool.spec),
                        exec_fn=_create_genie_exec_fn(
                            server_url, tool.name, poll_tool_names[space_id], w
                        ),
                    )
                )
                continue
        wrapped.append(tool)

    return wrapped


async def create_mcp_tools(
    w: WorkspaceClient,
    url_list: list[str],
    tool_description_overrides: dict[str, str] | None = None,
) -> list[ToolInfo]:
    """Create tools from MCP servers.

    For Genie MCP servers, async query/poll tool pairs are automatically collapsed
    into a single synchronous tool — the LLM only sees the query tool.

    Args:
        w: Databricks workspace client
        url_list: List of MCP server URLs
        tool_description_overrides: Optional map of tool name → description used
            to replace the MCP-advertised description. Llama-4-Maverick falls
            back to emitting tool calls as plain text when the advertised
            description is too terse, so this lets the project surface a
            richer description without editing the MCP server.

    Returns:
        List of ToolInfo objects
    """
    overrides = tool_description_overrides or {}
    tools = []
    for server_url in url_list:
        mcp_client = DatabricksMCPClient(server_url=server_url, workspace_client=w)
        mcp_tools = mcp_client.list_tools()
        server_tools = []
        for mcp_tool in mcp_tools:
            input_schema = mcp_tool.inputSchema.copy() if mcp_tool.inputSchema else {}
            description = (
                overrides.get(mcp_tool.name)
                or mcp_tool.description
                or f"Tool: {mcp_tool.name}"
            )
            tool_spec = {
                "type": "function",
                "function": {
                    "name": mcp_tool.name,
                    "parameters": input_schema,
                    "description": description,
                },
            }
            exec_fn = create_managed_exec_fn(server_url, mcp_tool.name, w)
            server_tools.append(
                ToolInfo(name=mcp_tool.name, spec=tool_spec, exec_fn=exec_fn)
            )

        tools.extend(_wrap_genie_tools(server_tools, server_url, w))

    return tools
