import mlflow
from mlflow.models import ModelConfig

from global_findex_curator.agent import FindexAgent

config = ModelConfig(
    development_config={
        "catalog": "mlops_dev",
        "schema": "corretco",
        "genie_space_id": "01f12b4f1c881997a13d98811a9617ca",
        "system_prompt": "prompt placeholder",
        "llm_endpoint": "databricks-gpt-oss-120b",
        "lakebase_project_id": "global-findex-agent-lakebase",
        "vs_tool_description": None,
    }
)

agent = FindexAgent(
    llm_endpoint=config.get("llm_endpoint"),
    system_prompt=config.get("system_prompt"),
    catalog=config.get("catalog"),
    schema=config.get("schema"),
    genie_space_id=config.get("genie_space_id"),
    lakebase_project_id=config.get("lakebase_project_id"),
    vs_tool_description=config.get("vs_tool_description"),
)
mlflow.models.set_model(agent)
