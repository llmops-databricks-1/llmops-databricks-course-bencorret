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
import os
import re
import time

from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
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
        self.global_findex_chunks_table = f"{self.catalog}.{self.schema}.global_findex_chunks_table"

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

        logger.info(
            f"Parsed PDFs from {self.pdf_dir} and saved to {self.parsed_table}"
        )

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

        df = self.spark.table(self.parsed_table).where(
            f"processed = {self.end}"
        )

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
            .withColumn(
                "chunks", extract_chunks_udf(col("parsed_content"))
            )
            .withColumn("chunk", explode(col("chunks")))
            .select(
                col("id"),
                col("chunk.chunk_id").alias("chunk_id"),
                clean_chunk_udf(col("chunk.content")).alias("text"),
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
