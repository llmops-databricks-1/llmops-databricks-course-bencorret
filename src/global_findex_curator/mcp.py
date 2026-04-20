"""MCP (Model Context Protocol) integration utilities."""

import concurrent.futures
import json
import time
from collections.abc import Callable

from databricks.sdk import WorkspaceClient
from databricks_mcp import DatabricksMCPClient
from pydantic import BaseModel

_GENIE_QUERY_PREFIX = "query_space_"
_GENIE_POLL_PREFIX = "poll_response_"
_GENIE_COMPLETE_STATUSES = {"COMPLETED", "FAILED", "CANCELLED"}

_DEFAULT_CALL_TIMEOUT = 120.0


# Needed to handle timeouts when calling Genie spaces
def _call_tool_with_timeout(client: DatabricksMCPClient, tool_name: str, arguments: dict, timeout: float) -> str:
    """Call an MCP tool with a hard timeout, raising TimeoutError if exceeded."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(client.call_tool, tool_name, arguments)
        try:
            result = future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            raise TimeoutError(f"MCP tool '{tool_name}' timed out after {timeout}s")
    return "".join([c.text for c in result.content])


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

    def exec_fn(**kwargs: dict) -> str:
        client = DatabricksMCPClient(server_url=server_url, workspace_client=w)
        response = client.call_tool(tool_name, kwargs)
        return "".join([c.text for c in response.content])

    return exec_fn


def _create_genie_exec_fn(
    server_url: str,
    query_tool_name: str,
    poll_tool_name: str,
    w: WorkspaceClient,
    max_polls: int = 30,
    poll_interval: float = 2.0,
    call_timeout: float = _DEFAULT_CALL_TIMEOUT,
) -> Callable:
    """Create an exec_fn for a Genie tool that handles async polling internally.

    The Genie MCP is asynchronous: query_space_* returns a pending status with
    conversation_id and message_id. This wrapper polls poll_response_* in a loop
    until the response reaches a terminal state, so the LLM only sees one tool.

    Args:
        server_url: MCP server URL for the Genie space
        query_tool_name: Name of the query_space_* tool
        poll_tool_name: Name of the poll_response_* tool
        w: Databricks workspace client
        max_polls: Maximum number of polling attempts before giving up
        poll_interval: Seconds to wait between polling attempts

    Returns:
        Callable that queries Genie and blocks until a result is available
    """

    def exec_fn(**kwargs: dict) -> str:
        client = DatabricksMCPClient(server_url=server_url, workspace_client=w)

        result_text = _call_tool_with_timeout(client, query_tool_name, kwargs, call_timeout)

        try:
            result = json.loads(result_text)
        except (json.JSONDecodeError, ValueError):
            return result_text

        status = result.get("status", "COMPLETED")
        conversation_id = result.get("conversationId") or result.get("conversation_id")
        message_id = result.get("messageId") or result.get("message_id")

        for _ in range(max_polls):
            if (
                status in _GENIE_COMPLETE_STATUSES
                or not conversation_id
                or not message_id
            ):
                break
            time.sleep(poll_interval)
            result_text = _call_tool_with_timeout(
                client,
                poll_tool_name,
                {"conversation_id": conversation_id, "message_id": message_id},
                call_timeout,
            )
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
            message_id = result.get("messageId") or result.get("message_id") or message_id

        return result_text

    return exec_fn


def _wrap_genie_tools(
    tools: list[ToolInfo], server_url: str, w: WorkspaceClient
) -> list[ToolInfo]:
    """Replace async Genie query/poll pairs with a single synchronous tool.

    Identifies query_space_* / poll_response_* pairs by their shared Genie space ID
    suffix, wraps the query tool with internal polling, and removes the poll tool
    from the list so the LLM never sees it.

    Args:
        tools: Tools loaded from a single MCP server
        server_url: MCP server URL (passed through to the new exec_fn)
        w: Databricks workspace client

    Returns:
        Tools list with poll tools removed and query tools wrapped
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
                        spec=tool.spec,
                        exec_fn=_create_genie_exec_fn(
                            server_url, tool.name, poll_tool_names[space_id], w
                        ),
                    )
                )
                continue
        wrapped.append(tool)

    return wrapped


async def create_mcp_tools(w: WorkspaceClient, url_list: list[str]) -> list[ToolInfo]:
    """Create tools from MCP servers.

    For Genie MCP servers, async query/poll tool pairs are automatically collapsed
    into a single synchronous tool — the LLM only sees the query tool.

    Args:
        w: Databricks workspace client
        url_list: List of MCP server URLs

    Returns:
        List of ToolInfo objects
    """
    tools = []
    for server_url in url_list:
        mcp_client = DatabricksMCPClient(server_url=server_url, workspace_client=w)
        mcp_tools = mcp_client.list_tools()
        server_tools = []
        for mcp_tool in mcp_tools:
            input_schema = mcp_tool.inputSchema.copy() if mcp_tool.inputSchema else {}
            tool_spec = {
                "type": "function",
                "function": {
                    "name": mcp_tool.name,
                    "parameters": input_schema,
                    "description": mcp_tool.description or f"Tool: {mcp_tool.name}",
                },
            }
            exec_fn = create_managed_exec_fn(server_url, mcp_tool.name, w)
            server_tools.append(
                ToolInfo(name=mcp_tool.name, spec=tool_spec, exec_fn=exec_fn)
            )

        tools.extend(_wrap_genie_tools(server_tools, server_url, w))

    return tools
