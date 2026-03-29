# Databricks notebook source
import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import DatabaseInstance, DatabaseInstanceState
from uuid import uuid4
from loguru import logger

from global_findex_curator.memory import LakebaseMemory
from global_findex_curator.config import load_config, get_env


scope_name = "global-findex-agent-scope"
os.environ["DATABRICKS_CLIENT_ID"] = dbutils.secrets.get(scope_name, "client_id")
os.environ["DATABRICKS_CLIENT_SECRET"] = dbutils.secrets.get(scope_name, "client_secret")


w = WorkspaceClient()
os.environ["DATABRICKS_HOST"] = w.config.host

# COMMAND ----------
instance_name = "global-findex-agent-instance"
instance = w.database.get_database_instance(instance_name)
lakebase_host = instance.read_write_dns

memory = LakebaseMemory(
    host=lakebase_host,
    instance_name=instance_name,
)

# COMMAND ----------

# Create a test session
session_id = f"test-session-{uuid4()}"

# Save some messages
test_messages = [
    {"role": "user", "content": "What does the Global Findex report say about financial inclusion?"},
    {"role": "assistant", "content": "The Global Findex report shows that account ownership has grown significantly..."},
    {"role": "user", "content": "Tell me more about trends in Sub-Saharan Africa"},
]

memory.save_messages(session_id, test_messages)
logger.info(f"✓ Saved {len(test_messages)} messages to session: {session_id}")

# COMMAND ----------

# Load messages back
loaded_messages = memory.load_messages(session_id)
