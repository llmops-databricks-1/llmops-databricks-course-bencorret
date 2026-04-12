# Databricks notebook source
# MAGIC %md
# MAGIC # Data Processing Pipeline
# MAGIC
# MAGIC This notebook processes Global Findex data and reports and syncs the vector search index.
# MAGIC
# MAGIC Pipeline steps:
# MAGIC 1. Ingest CSV data from volume into Delta tables
# MAGIC 2. Build the `global_findex_documents` metadata table
# MAGIC 3. Parse PDFs with AI Parse Document and extract cleaned chunks
# MAGIC 4. Sync the Vector Search index

# COMMAND ----------

from datetime import datetime

from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from global_findex_curator.config import get_env, load_config
from global_findex_curator.data_processor import DataProcessor
from global_findex_curator.vector_search import VectorSearchManager

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

env = get_env(spark)
cfg = load_config("../../project_config.yml", env=env)

CATALOG = cfg.catalog
SCHEMA = cfg.schema
VOLUME = cfg.volume
DOCUMENTS_TABLE = f"{CATALOG}.{SCHEMA}.global_findex_documents"

logger.info("Configuration loaded:")
logger.info(f"  Environment: {env}")
logger.info(f"  Catalog:     {CATALOG}")
logger.info(f"  Schema:      {SCHEMA}")
logger.info(f"  Volume:      {VOLUME}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Ingest CSV data into Delta tables

# COMMAND ----------

csv_paths = [
    f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/csv/findex_microdata_2025.csv",
    f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/csv/global_findex_database_2025.csv",
]

for csv_path in csv_paths:
    table_name = csv_path.split("/")[-1].replace(".csv", "")
    table_path = f"{CATALOG}.{SCHEMA}.{table_name}"
    logger.info(f"Ingesting {csv_path} -> {table_path}")
    df = spark.read.csv(csv_path, header=True, inferSchema=True)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(table_path)
    )
    logger.info(f"Wrote {df.count()} rows to {table_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build the Global Findex documents metadata table

# COMMAND ----------


def define_global_findex_documents() -> list[dict]:
    """Static metadata for all Global Findex source documents."""
    ingestion_timestamp = datetime.now().isoformat()
    volume_root = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
    authors = ["Leora Klapper", "Asli Demirguc-Kunt", "Douglas Randall"]

    return [
        {
            "id": "findex_microdata_2025.csv",
            "title": "Findex microdata 2025 full survey dataset",
            "authors": authors,
            "summary": (
                "Raw microdata from the 2025 Global Findex survey. Contains 144,090 "
                "individual respondent records with cryptic column names representing "
                "survey questions and respondent metadata (country, sex, etc.). Each "
                "row is one survey respondent."
            ),
            "published": "2025-01-01 00:00",
            "updated": None,
            "categories": "Survey data, Raw data, Mobile integration, Financial integration",
            "document_type": "CSV",
            "primary_category": "Survey data",
            "ingestion_timestamp": ingestion_timestamp,
            "processed": None,
            "volume_path": f"{volume_root}/csv/findex_microdata_2025/findex_microdata_2025.csv",
        },
        {
            "id": "documentation_microdata",
            "title": "Documentation of microdata survey",
            "authors": authors,
            "summary": (
                "PDF documentation for all columns in the microdata database. Each "
                "cryptic column name (e.g. 'con31e') is described with its label, the "
                "question asked to respondents, and the possible answer values. "
                "Required to interpret the microdata CSV."
            ),
            "published": "2025-01-01 00:00",
            "updated": None,
            "categories": "Data Documentation, Survey Methodology, Financial Inclusion",
            "document_type": "PDF",
            "primary_category": "Data Documentation",
            "ingestion_timestamp": ingestion_timestamp,
            "processed": None,
            "volume_path": f"{volume_root}/pdf/documentation_microdata/documentation_microdata.pdf",
        },
        {
            "id": "Global_findex_-_The_little_data_book",
            "title": "Global findex - The little data book",
            "authors": authors,
            "summary": (
                "A compact PDF presenting aggregated financial inclusion KPIs from the "
                "2025 Global Findex survey, broken down by income category, world "
                "region, and individual country. Designed for quick cross-country and "
                "cross-region comparisons."
            ),
            "published": "2025-01-01 00:00",
            "updated": None,
            "categories": "Financial Inclusion, Regional Analysis, Country Comparisons, Income Categories",
            "document_type": "PDF",
            "primary_category": "Financial Inclusion",
            "ingestion_timestamp": ingestion_timestamp,
            "processed": None,
            "volume_path": f"{volume_root}/pdf/Global_findex_-_The_little_data_book/Global_findex_-_The_little_data_book.pdf",
        },
        {
            "id": "Global_findex_2025_-_Executive_report",
            "title": "Global findex 2025 - Executive report",
            "authors": authors,
            "summary": (
                "A 56-page executive summary of the 2025 Global Findex survey findings. "
                "Covers key topics including mobile phone ownership among adults, gender "
                "gaps in account ownership, savings per adult, and digital merchant "
                "payments. Aimed at a broad audience."
            ),
            "published": "2025-01-01 00:00",
            "updated": None,
            "categories": "Financial Inclusion, Digital Payments, Gender Equality, Mobile Banking",
            "document_type": "PDF",
            "primary_category": "Financial Inclusion",
            "ingestion_timestamp": ingestion_timestamp,
            "processed": None,
            "volume_path": f"{volume_root}/pdf/Global_findex_2025_-_Executive_report/Global_findex_2025_-_Executive_report.pdf",
        },
        {
            "id": "Global_findex_database_2025",
            "title": "Global findex database 2025",
            "authors": authors,
            "summary": (
                "The full 342-page 2025 Global Findex report. Covers in depth all "
                "survey conclusions across major sections: Financial Access, Financial "
                "Use, and Financial Health. Intended for researchers and readers "
                "seeking deep analysis beyond what the executive report provides."
            ),
            "published": "2025-01-01 00:00",
            "updated": None,
            "categories": "Financial Access, Financial Use, Financial Health, Financial Inclusion",
            "document_type": "PDF",
            "primary_category": "Financial Inclusion",
            "ingestion_timestamp": ingestion_timestamp,
            "processed": None,
            "volume_path": f"{volume_root}/pdf/Global_findex_database_2025/Global_findex_database_2025.pdf",
        },
        {
            "id": "global_findex_database_2025.csv",
            "title": "Global findex 2025 curated dataset",
            "authors": authors,
            "summary": (
                "A curated and enriched CSV derived from the microdata database, "
                "containing 8,564 records with additional socio-economic headers. "
                "Likely a refined subset of the full microdata intended for easier "
                "analytical use."
            ),
            "published": "2025-01-01 00:00",
            "updated": None,
            "categories": "Survey data, Enriched data, Mobile integration, Financial integration",
            "document_type": "CSV",
            "primary_category": "Survey data",
            "ingestion_timestamp": ingestion_timestamp,
            "processed": None,
            "volume_path": f"{volume_root}/csv/Global_findex_database_2025/Global_findex_database_2025.csv",
        },
    ]


documents_schema = StructType(
    [
        StructField("id", StringType(), False),
        StructField("title", StringType(), False),
        StructField("authors", ArrayType(StringType()), True),
        StructField("summary", StringType(), True),
        StructField("published", StringType(), True),
        StructField("updated", StringType(), True),
        StructField("categories", StringType(), True),
        StructField("document_type", StringType(), True),
        StructField("primary_category", StringType(), True),
        StructField("ingestion_timestamp", StringType(), True),
        StructField("processed", LongType(), True),
        StructField("volume_path", StringType(), True),
    ]
)

documents = define_global_findex_documents()
documents_df = spark.createDataFrame(documents, schema=documents_schema)
(
    documents_df.write.format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(DOCUMENTS_TABLE)
)
logger.info(f"Wrote {documents_df.count()} document records to {DOCUMENTS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Parse PDFs and extract chunks

# COMMAND ----------

processor = DataProcessor(spark=spark, config=cfg)
processor.process_and_save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Sync Vector Search index

# COMMAND ----------

vs_manager = VectorSearchManager(config=cfg)
vs_manager.sync_index()

logger.info("Data processing pipeline complete.")
