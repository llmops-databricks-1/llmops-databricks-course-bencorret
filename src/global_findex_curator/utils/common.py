import os
from pathlib import Path

import mlflow
from delta.tables import DeltaTable
from dotenv import load_dotenv
from pyspark.sql import SparkSession


def resolve_path(relative_path: str, search_levels: int = 3) -> str:
    """Resolve a relative path by searching up the directory tree.

    Useful for notebooks where the working directory differs between
    local and Databricks environments.

    Args:
        relative_path: Relative path to resolve (e.g. "../../notes/file.yaml")
        search_levels: Number of parent directory levels to search

    Returns:
        Resolved absolute path string, or the original path if not found
    """
    if Path(relative_path).is_absolute():
        return relative_path
    current = Path.cwd()
    for _ in range(search_levels):
        candidate = current / relative_path
        if candidate.exists():
            return str(candidate)
        current = current.parent
    return relative_path


def get_widget(name: str, default: str | None = None) -> str | None:
    """Get a Databricks widget value with a fallback default.

    :param name: Widget name.
    :param default: Default value if widget is not set.
    :return: Widget value or default.
    """
    from databricks.sdk.runtime import dbutils

    try:
        return dbutils.widgets.get(name)
    except KeyError:
        return default


def set_mlflow_tracking_uri() -> None:
    """
    Set the MLflow tracking URI based on the provided profile.

    """
    if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
        load_dotenv()
        profile = os.environ["PROFILE"]
        mlflow.set_tracking_uri(f"databricks://{profile}")
        mlflow.set_registry_uri(f"databricks-uc://{profile}")


def get_delta_table_version(spark: SparkSession, full_table_name: str) -> str:
    """
    Get the latest version of a Delta table.

    :param spark: Spark session.
    :param full_table_name: Name of the delta table as {catalog}.{schema}.{name}.
    :return: Latest version of the Delta table.
    """
    delta_table = DeltaTable.forName(spark, full_table_name)
    return str(delta_table.history().select("version").first()[0])
