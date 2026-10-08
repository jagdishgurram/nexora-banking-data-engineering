from pyspark import pipelines as dp
import pyspark.sql.functions as F
from Setup.Parameter import DATASETS

# Dataset parameters are imported from the centralized configuration file.
raw = spark.conf.get("source.base_path")

# Loop through all configured datasets and create Bronze tables dynamically.
for dataset in DATASETS:
    @dp.table(
        name=f"nexora_catalog.bronze.raw_{dataset}",
        # Each dataset is created as a separate Bronze streaming table.
        comment=f"Streaming ingestion of raw {dataset} data with Auto Loader",
        table_properties={
            "quality": "bronze",
            "layer": "bronze",
            "source_format": "csv",
            "delta.enableChangeDataFeed": "true"
        }
    )
    def transactions_bronze(dataset=dataset):
        df = (
            # Auto Loader with Structured Streaming incrementally reads new CSV files.
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation", f"{raw}/_schema/{dataset}")
            # Rescue unexpected schema changes instead of failing ingestion.
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .load(f"{raw}/{dataset}")
        )

        # Add source-file and ingestion-time metadata for auditing and traceability.
        df = df.withColumn("file_name", F.col("_metadata.file_name")).withColumn("ingest_datetime", F.current_timestamp())
        return df