# Databricks notebook source

from datetime import datetime

from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

from global_findex_curator.config import get_env, load_config

# COMMAND ----------
# Create Spark session
spark = SparkSession.builder.getOrCreate()

# Load config
env = get_env(spark)
cfg = load_config("../project_config.yml", env)

CATALOG = cfg.catalog
SCHEMA = cfg.schema
TABLE_NAME = "global_findex_documents"

# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
logger.info(f"Schema {CATALOG}.{SCHEMA} ready")

# COMMAND ----------
# Define documents metadata


def define_global_findex_documents():
    """
    Fetch generated documents metadata. Hardcoded.

    Returns:
        List of document metadata dictionaries
    """
    documents = [
        {
            "id": "1",
            "title": "findex_microdata_2025",
            "authors": ["Leora Klapper", "Asli Demirguc-Kunt", "Douglas Randall"],
            "summary": "Raw microdata from the 2025 Global Findex survey. Contains 144,090 individual respondent records with cryptic column names representing survey questions and respondent metadata (country, sex, etc.). Each row is one survey respondent.",
            "published": "2025-01-01 00:00",
            "updated": None,
            "categories": "Survey data, Raw data, Mobile integration, Financial integration",
            "document_type": "CSV",
            "primary_category": "Survey data",
            "ingestion_timestamp": datetime.now().isoformat(),
            "processed": None,
            "volume_path": f"/Volumes/llmops_{env}/global_findex/global_findex_files/findex_microdata_2025.csv",
        },
        {
            "id": "2",
            "title": "documentation_microdata",
            "authors": ["Leora Klapper", "Asli Demirguc-Kunt", "Douglas Randall"],
            "summary": "PDF documentation for all columns in the microdata database. Each cryptic column name (e.g. 'con31e') is described with its label, the question asked to respondents, and the possible answer values. Required to interpret the microdata CSV.",
            "published": "2025-01-01 00:00",
            "updated": None,
            "categories": "Data Documentation, Survey Methodology, Financial Inclusion",
            "document_type": "PDF",
            "primary_category": "Data Documentation",
            "ingestion_timestamp": datetime.now().isoformat(),
            "processed": None,
            "volume_path": f"/Volumes/llmops_{env}/global_findex/global_findex_files/documentation_microdata.pdf",
        },
        {
            "id": "3",
            "title": "Global findex - The little data book",
            "authors": ["Leora Klapper", "Asli Demirguc-Kunt", "Douglas Randall"],
            "summary": "A compact PDF presenting aggregated financial inclusion KPIs from the 2025 Global Findex survey, broken down by income category, world region, and individual country. Designed for quick cross-country and cross-region comparisons.",
            "published": "2025-01-01 00:00",
            "updated": None,
            "categories": "Financial Inclusion, Regional Analysis, Country Comparisons, Income Categories",
            "document_type": "PDF",
            "primary_category": "Financial Inclusion",
            "ingestion_timestamp": datetime.now().isoformat(),
            "processed": None,
            "volume_path": f"/Volumes/llmops_{env}/global_findex/global_findex_files/Global findex - The little data book.pdf",
        },
        {
            "id": "4",
            "title": "Global findex 2025 - Executive report",
            "authors": ["Leora Klapper", "Asli Demirguc-Kunt", "Douglas Randall"],
            "summary": "A 56-page executive summary of the 2025 Global Findex survey findings. Covers key topics including mobile phone ownership among adults, gender gaps in account ownership, savings per adult, and digital merchant payments. Aimed at a broad audience.",
            "published": "2025-01-01 00:00",
            "updated": None,
            "categories": "Financial Inclusion, Digital Payments, Gender Equality, Mobile Banking",
            "document_type": "PDF",
            "primary_category": "Financial Inclusion",
            "ingestion_timestamp": datetime.now().isoformat(),
            "processed": None,
            "volume_path": f"/Volumes/llmops_{env}/global_findex/global_findex_files/Global findex 2025 - Executive report.pdf",
        },
        {
            "id": "5",
            "title": "Global findex database 2025",
            "authors": ["Leora Klapper", "Asli Demirguc-Kunt", "Douglas Randall"],
            "summary": "The full 342-page 2025 Global Findex report. Covers in depth all survey conclusions across major sections: Financial Access, Financial Use, and Financial Health. Intended for researchers and readers seeking deep analysis beyond what the executive report provides.",
            "published": "2025-01-01 00:00",
            "updated": None,
            "categories": "Financial Access, Financial Use, Financial Health, Financial Inclusion",
            "document_type": "PDF",
            "primary_category": "Financial Inclusion",
            "ingestion_timestamp": datetime.now().isoformat(),
            "processed": None,
            "volume_path": f"/Volumes/llmops_{env}/global_findex/global_findex_files/Global findex database 2025.pdf",
        },
        {
            "id": "6",
            "title": "global_findex_database_2025",
            "authors": ["Leora Klapper", "Asli Demirguc-Kunt", "Douglas Randall"],
            "summary": "A curated and enriched CSV derived from the microdata database, containing 8,564 records with additional socio-economic headers. Likely a refined subset of the full microdata intended for easier analytical use.",
            "published": "2025-01-01 00:00",
            "updated": None,
            "categories": "Survey data, Enriched data, Mobile integration, Financial integration",
            "document_type": "CSV",
            "primary_category": "Survey data",
            "ingestion_timestamp": datetime.now().isoformat(),
            "processed": None,
            "volume_path": f"/Volumes/llmops_{env}/global_findex/global_findex_files/global_findex_database_2025.csv",
        },
    ]

    return documents


logger.info("Generating documents metadata...")
documents = define_global_findex_documents()
logger.info(f"Fetched {len(documents)} documents")
logger.info("Sample document:")
logger.info(f"Title: {documents[0]['title']}")
logger.info(f"Authors: {documents[0]['authors']}")
logger.info(f"ID: {documents[0]['id']}")
logger.info(f"Document Type: {documents[0]['document_type']}")

# COMMAND ----------
# Create Delta Table in Unity Catalog
# Store the Global Findex documents metadata in a Delta table for downstream processing.

# Define schema
schema = StructType(
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
        StructField("volume_path", StringType(), True),  # Will be set in Lecture 2.2
    ]
)

# Create DataFrame
df = spark.createDataFrame(documents, schema=schema)

# Write to Delta table
table_path = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"

df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(
    table_path
)

logger.info(f"Created Delta table: {table_path}")
logger.info(f"Records: {df.count()}")

# COMMAND ----------
# Verify the Data

# Read back the table
documents_df = spark.table(f"{CATALOG}.{SCHEMA}.{TABLE_NAME}")

logger.info(f"Table: {CATALOG}.{SCHEMA}.{TABLE_NAME}")
logger.info(f"Total documents: {documents_df.count()}")
logger.info("Schema:")
documents_df.printSchema()

logger.info("Sample records:")
documents_df.select("id", "title", "primary_category", "published").show(5, truncate=50)

# COMMAND ----------
# Data Statistics

logger.info("Documents by primary category:")
documents_df.groupBy("primary_category").count().orderBy("count", ascending=False).show()

logger.info("Most recent documents:")
documents_df.select("title", "published", "id").orderBy(
    "published", ascending=False
).show(5, truncate=60)
