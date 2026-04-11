# Databricks notebook source


from loguru import logger
from pyspark.sql import SparkSession

from global_findex_curator.config import get_env, load_config

# COMMAND ----------
# Create Spark session
spark = SparkSession.builder.getOrCreate()

# Load config
env = get_env(spark)
cfg = load_config("../project_config.yml", env)

CATALOG = cfg.catalog
SCHEMA = cfg.schema

# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
logger.info(f"Schema {CATALOG}.{SCHEMA} ready")

# COMMAND ----------
# Write CSV data to tables, overwrite mode
csv_paths = [
    f"/Volumes/mlops_{env}/corretco/global_findex_volume/csv/findex_microdata_2025.csv",
    f"/Volumes/mlops_{env}/corretco/global_findex_volume/csv/global_findex_database_2025.csv",
]

for csv_path in csv_paths:
    table_path = f"{CATALOG}.{SCHEMA}.{csv_path.split('/')[-1].replace('.csv', '')}"
    logger.info(f"Will overwrite data to table {table_path}")
    df = spark.read.csv(csv_path, header=True, inferSchema=True)
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(
        table_path
    )
    logger.info(f"Wrote data from {csv_path} to table {table_path}")
    logger.info(f"Wrote {df.count()} rows to table {table_path}")
