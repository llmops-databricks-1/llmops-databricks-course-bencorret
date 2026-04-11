# Databricks notebook source
# MAGIC %md
# MAGIC # Lecture 3.3: Global Findex Research Assistant Agent
# MAGIC
# MAGIC ## Topics Covered:
# MAGIC - Building a research assistant with MCP tools
# MAGIC - Tool routing: Vector Search vs Genie Space
# MAGIC - Conversation history management
# MAGIC - Multi-turn agent interactions
# MAGIC
# MAGIC **Agent Architecture:**
# MAGIC ```
# MAGIC User Question
# MAGIC     ↓
# MAGIC System Prompt (tool routing guidance)
# MAGIC     ↓
# MAGIC LLM decides which tool(s) to call
# MAGIC     ├── Vector Search MCP → report content, findings, methodology
# MAGIC     └── Genie Space MCP  → statistics, country comparisons, demographics
# MAGIC     ↓
# MAGIC LLM generates answer from tool results
# MAGIC     ↓
# MAGIC Response (stored in conversation history)
# MAGIC ```

# COMMAND ----------

import asyncio
import json

import nest_asyncio
from databricks.sdk import WorkspaceClient
from loguru import logger
from openai import OpenAI
from pyspark.sql import SparkSession

from global_findex_curator.config import get_env, load_config
from global_findex_curator.mcp import create_mcp_tools

# Enable nested event loops (required for Databricks notebooks)
nest_asyncio.apply()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration Setup

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# Load configuration
env = get_env(spark)
cfg = load_config("../project_config.yml", env)

w = WorkspaceClient()

logger.info(f"Environment: {env}")
logger.info(f"Catalog: {cfg.catalog}, Schema: {cfg.schema}")
logger.info(f"LLM endpoint: {cfg.llm_endpoint}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. MCP Tool Registry
# MAGIC
# MAGIC We load tools from two MCP servers:
# MAGIC 1. **Vector Search MCP** — exposes our `global_findex_index` for semantic search over parsed report chunks
# MAGIC 2. **Genie Space MCP** — exposes natural-language SQL over the survey microdata

# COMMAND ----------

host = w.config.host

# Build MCP server URLs
mcp_urls = [f"{host}/api/2.0/mcp/vector-search/{cfg.catalog}/{cfg.schema}"]

if cfg.genie_space_id:
    mcp_urls.append(f"{host}/api/2.0/mcp/genie/{cfg.genie_space_id}")
else:
    logger.warning(
        "Genie space not configured — only Vector Search tools will be available"
    )

logger.info(f"Loading tools from {len(mcp_urls)} MCP server(s)...")

# Load all tools
mcp_tools = asyncio.run(create_mcp_tools(w, mcp_urls))

logger.info(f"Loaded {len(mcp_tools)} tool(s):")
for i, tool in enumerate(mcp_tools, 1):
    logger.info(f"  {i}. {tool.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. System Prompt
# MAGIC
# MAGIC The system prompt tells the agent **when** to use each tool.
# MAGIC - Questions about report content, findings, or methodology → Vector Search
# MAGIC - Questions about specific statistics or numerical comparisons → Genie

# COMMAND ----------

SYSTEM_PROMPT = """You are a Global Findex Research Assistant. You help researchers explore the World Bank's Global Findex database, which measures how adults around the world save, borrow, make payments, and manage financial risk.

You have access to two types of tools:

1. **Vector Search tool** (name contains "global_findex_index"): Use this for questions about:
   - Report content, findings, and methodology
   - Qualitative insights and policy recommendations
   - Definitions and concepts from the Global Findex reports
   - Trends and narratives described in the publications

2. **Genie tool** (name contains "genie"): Use this for questions about:
   - Specific statistics and numerical data from the survey
   - Country-level or region-level comparisons
   - Data breakdowns by gender, income, age, or other demographics
   - Quantitative trends across survey waves

Guidelines:
- Always use the appropriate tool before answering. Do not guess at data.
- If a question involves both qualitative context and specific numbers, call both tools.
- Cite the source of your information (report content vs survey data).
- If the tools return no relevant results, say so honestly.
- Keep answers concise and well-structured for a research audience.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Agent Class
# MAGIC
# MAGIC `GlobalFindexAgent` combines:
# MAGIC - MCP tool calling loop (from notebook 3.2)
# MAGIC - In-memory conversation history as a simple Python list (from notebook 3.1b)

# COMMAND ----------


class GlobalFindexAgent:
    """Research assistant agent with MCP tools and conversation history."""

    def __init__(
        self,
        llm_endpoint: str,
        system_prompt: str,
        tools: list,
        workspace_client: WorkspaceClient,
    ):
        self.llm_endpoint = llm_endpoint
        self.system_prompt = system_prompt
        self._tools_dict = {tool.name: tool for tool in tools}
        self._client = OpenAI(
            api_key=workspace_client.tokens.create(lifetime_seconds=1200).token_value,
            base_url=f"{workspace_client.config.host}/serving-endpoints",
        )
        self.conversation_history: list[dict] = []

    def get_tool_specs(self) -> list[dict]:
        """Get tool specifications for the LLM."""
        return [tool.spec for tool in self._tools_dict.values()]

    def execute_tool(self, tool_name: str, args: dict) -> str:
        """Execute a tool by name."""
        if tool_name not in self._tools_dict:
            raise ValueError(f"Unknown tool: {tool_name}")
        return self._tools_dict[tool_name].exec_fn(**args)

    def chat(self, user_message: str, max_iterations: int = 10) -> str:
        """Send a message and get a response, with automatic tool calling.

        Conversation history persists across calls. Tool call/result messages
        are kept local to the current invocation to avoid context bloat.
        """
        # Add user message to persistent history
        self.conversation_history.append({"role": "user", "content": user_message})

        # Build messages: system + history (the LLM sees the full conversation so far)
        messages = [
            {"role": "system", "content": self.system_prompt}
        ] + self.conversation_history

        for _iteration in range(max_iterations):
            response = self._client.chat.completions.create(
                model=self.llm_endpoint,
                messages=messages,
                tools=self.get_tool_specs() if self._tools_dict else None,
            )

            assistant_message = response.choices[0].message

            if assistant_message.tool_calls:
                # Append assistant message with tool calls to local messages
                messages.append(
                    {
                        "role": "assistant",
                        "content": assistant_message.content,
                        "tool_calls": [
                            {
                                "id": tc.id,
                                "type": "function",
                                "function": {
                                    "name": tc.function.name,
                                    "arguments": tc.function.arguments,
                                },
                            }
                            for tc in assistant_message.tool_calls
                        ],
                    }
                )

                for tool_call in assistant_message.tool_calls:
                    tool_name = tool_call.function.name
                    tool_args = json.loads(tool_call.function.arguments)

                    logger.info(f"Calling tool: {tool_name}({tool_args})")

                    try:
                        result = self.execute_tool(tool_name, tool_args)
                    except Exception as e:
                        result = f"Error: {e}"

                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tool_call.id,
                            "content": str(result),
                        }
                    )
            else:
                # Final answer — persist to conversation history
                self.conversation_history.append(
                    {"role": "assistant", "content": assistant_message.content}
                )
                return assistant_message.content

        return "Max iterations reached."

    def clear_history(self) -> None:
        """Clear conversation history."""
        self.conversation_history = []


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create the Agent

# COMMAND ----------

agent = GlobalFindexAgent(
    llm_endpoint=cfg.llm_endpoint,
    system_prompt=SYSTEM_PROMPT,
    tools=mcp_tools,
    workspace_client=w,
)

logger.info("Agent created with tools:")
for name in agent._tools_dict:
    logger.info(f"  - {name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Test the Agent
# MAGIC
# MAGIC ### Test 1: Report content question (should use Vector Search)

# COMMAND ----------

response = agent.chat(
    "What do the Global Findex reports say about barriers to financial inclusion?"
)
logger.info(f"Response:\n{response}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 2: Statistical question (should use Genie)

# COMMAND ----------

response = agent.chat(
    "What percentage of adults in Sub-Saharan Africa have a bank account?"
)
logger.info(f"Response:\n{response}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 3: Follow-up question (tests conversation history)

# COMMAND ----------

response = agent.chat("How does that compare to other regions?")
logger.info(f"Response:\n{response}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test 4: Clear history

# COMMAND ----------

agent.clear_history()
logger.info(f"History cleared. Conversation length: {len(agent.conversation_history)}")
