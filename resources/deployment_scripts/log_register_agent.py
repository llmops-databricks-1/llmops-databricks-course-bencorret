# Databricks notebook source
import mlflow

from global_findex_curator.agent import log_register_agent
from global_findex_curator.config import ProjectConfig
from global_findex_curator.evaluation import evaluate_agent
from global_findex_curator.utils.common import get_widget

env = get_widget("env", "dev")
git_sha = get_widget("git_sha", "local")
run_id = get_widget("run_id", "local")

cfg = ProjectConfig.from_yaml(config_path="../../project_config.yml", env=env)

mlflow.set_experiment(cfg.experiment_name)

model_name = f"{cfg.catalog}.{cfg.schema}.global_findex_agent"

# COMMAND ----------
# Run evaluation
results = evaluate_agent(cfg, eval_inputs_path="../../eval_inputs.txt")

# COMMAND ----------
# Log and register model
registered_model = log_register_agent(
    cfg=cfg,
    git_sha=git_sha,
    run_id=run_id,
    agent_code_path="../../global_findex_agent.py",
    model_name=model_name,
    evaluation_metrics=results.metrics,
)
