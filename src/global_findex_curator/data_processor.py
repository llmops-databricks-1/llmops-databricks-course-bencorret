"""
PDFs in Volume + global_findex_documents table
   ↓ (parse_pdfs_with_ai)
ai_parsed_docs_table (JSON)
   ↓ (process_chunks)
global_findex_chunks_table (clean text + metadata)
   ↓ (VectorSearchManager - separate class) (2.4 notebook)
Vector Search Index (embeddings)
"""

import json
import re
import time
from functools import reduce
from pathlib import Path

import yaml
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    dayofmonth,
    explode,
    month,
    to_date,
    udf,
    year,
)
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from global_findex_curator.config import ProjectConfig


class DataProcessor:
    """
    DataProcessor handles the complete workflow of:
    - Parsing PDFs with ai_parse_document
    - Extracting and cleaning text chunks
    - Saving chunks to Delta tables
    """

    def __init__(self, spark: SparkSession, config: ProjectConfig) -> None:
        """
        Initialize DataProcessor with Spark session and configuration.

        Args:
            spark: SparkSession instance
            config: ProjectConfig object with table configurations
        """
        self.spark = spark
        self.cfg = config
        self.catalog = config.catalog
        self.schema = config.schema
        self.volume = config.volume

        self.end = time.strftime("%Y%m%d%H%M", time.gmtime())
        self.pdf_dir = f"/Volumes/{self.catalog}/{self.schema}/{self.volume}/pdf"
        self.documents_table = f"{self.catalog}.{self.schema}.global_findex_documents"
        self.parsed_table = f"{self.catalog}.{self.schema}.ai_parsed_docs_table"
        self.global_findex_chunks_table = (
            f"{self.catalog}.{self.schema}.global_findex_chunks_table"
        )

    def parse_pdfs_with_ai(self) -> None:
        """
        Parse PDFs using ai_parse_document and store in ai_parsed_docs table.

        """

        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.parsed_table} (
                path STRING,
                parsed_content STRING,
                processed LONG
            )
        """)

        # We do not add new PDF documents in this project, just use a static list
        # So we need to clear any exitsting data
        # Hence the OVERWRITE mode
        self.spark.sql(f"""
            INSERT OVERWRITE {self.parsed_table}
            SELECT
                path,
                ai_parse_document(content) AS parsed_content,
                {self.end} AS processed
            FROM READ_FILES(
                "{self.pdf_dir}",
                format => 'binaryFile'
            )
        """)

        logger.info(f"Parsed PDFs from {self.pdf_dir} and saved to {self.parsed_table}")

    @staticmethod
    def _extract_chunks(parsed_content_json: str) -> list[tuple[str, str]]:
        """
        Extract chunks from parsed_content JSON.

        Args:
            parsed_content_json: JSON string containing
            parsed document structure

        Returns:
            List of tuples containing (chunk_id, content)
        """
        parsed_dict = json.loads(parsed_content_json)
        chunks = []

        for element in parsed_dict.get("document", {}).get("elements", []):
            if element.get("type") == "text":
                chunk_id = element.get("id", "")
                content = element.get("content", "")
                chunks.append((chunk_id, content))

        return chunks

    @staticmethod
    def _extract_paper_id(path: str) -> str:
        """
        Extract paper ID from file path.

        Args:
            path: File path (e.g., "/path/to/paper_id.pdf")

        Returns:
            Paper ID extracted from the path
        """
        return path.replace(".pdf", "").split("/")[-1]

    @staticmethod
    def _clean_chunk(text: str) -> str:
        """
        Clean and normalize chunk text
        Args:
            text: Raw text content

        Returns:
            Cleaned text content
        """
        # Fix hyphenation across line breaks:
        # "docu-\nments" => "documents"
        t = re.sub(r"(\w)-\s*\n\s*(\w)", r"\1\2", text)

        # Collapse internal newlines into spaces
        t = re.sub(r"\s*\n\s*", " ", t)

        # Collapse repeated whitespace
        t = re.sub(r"\s+", " ", t)

        return t.strip()

    def process_chunks(self) -> None:
        """
        Process parsed documents to extract and clean chunks.
        Reads from ai_parsed_docs table and saves to global_findex_chunks table.
        """
        logger.info(
            f"Processing parsed documents from "
            f"{self.parsed_table} for end date {self.end}"
        )

        df = self.spark.table(self.parsed_table).where(f"processed = {self.end}")

        # Define schema for the extracted chunks
        chunk_schema = ArrayType(
            StructType(
                [
                    StructField("chunk_id", StringType(), True),
                    StructField("content", StringType(), True),
                ]
            )
        )

        extract_chunks_udf = udf(self._extract_chunks, chunk_schema)
        extract_paper_id_udf = udf(self._extract_paper_id, StringType())
        clean_chunk_udf = udf(self._clean_chunk, StringType())

        metadata_df = self.spark.table(self.documents_table).select(
            col("id"),
            col("title"),
            col("summary"),
            concat_ws(", ", col("authors")).alias("authors"),
            year(to_date(col("published"), "yyyy-MM-dd HH:mm")).alias("year"),
            month(to_date(col("published"), "yyyy-MM-dd HH:mm")).alias("month"),
            dayofmonth(to_date(col("published"), "yyyy-MM-dd HH:mm")).alias("day"),
        )

        # Create the transformed dataframe
        chunks_df = (
            df.withColumn("id", extract_paper_id_udf(col("path")))
            .withColumn("chunks", extract_chunks_udf(col("parsed_content")))
            .withColumn("chunk", explode(col("chunks")))
            .select(
                col("id"),
                col("chunk.chunk_id").alias("chunk_id"),
                clean_chunk_udf(col("chunk.content")).alias("text"),
                concat_ws("_", col("id"), col("chunk.chunk_id")).alias("unique_id"),
            )
            .join(metadata_df, "id", "left")
        )

        # Write to table
        chunks_df.write.mode("overwrite").saveAsTable(self.global_findex_chunks_table)
        logger.info(f"Saved chunks to {self.global_findex_chunks_table}")

        # Enable Change Data Feed
        self.spark.sql(f"""
            ALTER TABLE {self.global_findex_chunks_table}
            SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
        """)
        logger.info(f"Change Data Feed enabled for {self.global_findex_chunks_table}")

    def process_and_save(self) -> None:
        """
        Complete workflow: download papers, parse PDFs, and process chunks.
        """
        # Step 1: Parse PDFs with ai_parse_document
        self.parse_pdfs_with_ai()
        logger.info("Parsed documents.")

        # Step 2: Process chunks
        self.process_chunks()
        logger.info("Processing complete!")


class CsvDataProcessor:
    """
    Ingests a CSV file into a Delta table, renames columns and adds comments
    using a YAML variable dictionary.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: ProjectConfig,
        csv_path: str,
        yaml_path: str,
    ) -> None:
        """
        Initialize CsvDataProcessor.

        Args:
            spark: SparkSession instance
            config: ProjectConfig object with catalog/schema configuration
            csv_path: Absolute path to the CSV file (e.g. in a Databricks Volume)
            yaml_path: Absolute path to the YAML variable dictionary
        """
        self.spark = spark
        self.csv_path = csv_path
        self.catalog = config.catalog
        self.schema = config.schema

        table_name = Path(csv_path).stem
        self.table_path = f"{self.catalog}.{self.schema}.{table_name}"

        with open(yaml_path) as f:
            self._yaml_data = yaml.safe_load(f)

    def _build_rename_mapping(self) -> dict[str, str]:
        """Return mapping of original column name -> improved column name."""
        return {v["name"]: v["improved_col_name"] for v in self._yaml_data.values()}

    def _build_comment_mapping(self) -> dict[str, str]:
        """Return mapping of improved column name -> comment text."""
        comment_mapping = {}
        for v in self._yaml_data.values():
            col_name = v["improved_col_name"]
            label = v.get("label") or ""
            question = v.get("question") or ""
            if label and question:
                comment_mapping[col_name] = f"{label} | {question}"
            elif label:
                comment_mapping[col_name] = label
            elif question:
                comment_mapping[col_name] = question
        return comment_mapping

    def ingest(self) -> None:
        """Read CSV, rename columns using YAML mapping, and write to Delta table."""
        rename_mapping = self._build_rename_mapping()

        logger.info(f"Ingesting {self.csv_path} -> {self.table_path}")
        df = self.spark.read.csv(self.csv_path, header=True, inferSchema=True)

        df = reduce(
            lambda d, c: d.withColumnRenamed(c, rename_mapping[c]),
            [c for c in df.columns if c in rename_mapping and rename_mapping[c] != c],
            df,
        )
        logger.info(
            f"Renamed {sum(1 for k, v in rename_mapping.items() if k != v)} columns"
        )

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .option("overwriteSchema", "true")
            .saveAsTable(self.table_path)
        )
        logger.info(f"Wrote {df.count()} rows to {self.table_path}")

    def apply_comments(self) -> None:
        """Apply column comments from YAML if not already present on the table."""
        test_col = "has_bank_account"
        existing_comment = None
        for row in self.spark.sql(f"DESCRIBE TABLE {self.table_path}").collect():
            if row["col_name"] == test_col:
                existing_comment = row["comment"]
                break

        if existing_comment:
            logger.info(
                f"Column comments already present on {self.table_path} "
                f"(e.g. `{test_col}`: '{existing_comment[:60]}…'). Skipping."
            )
            return

        comment_mapping = self._build_comment_mapping()
        logger.info(
            f"Applying comments to {len(comment_mapping)} columns on {self.table_path}"
        )

        for col_name, comment_text in comment_mapping.items():
            escaped_comment = comment_text.replace("'", "\\'")
            self.spark.sql(
                f"ALTER TABLE {self.table_path} ALTER COLUMN `{col_name}` COMMENT '{escaped_comment}'"
            )

        logger.info("Column comments applied successfully")

    def process(self) -> None:
        """Full workflow: ingest CSV and apply column comments."""
        self.ingest()
        self.apply_comments()
