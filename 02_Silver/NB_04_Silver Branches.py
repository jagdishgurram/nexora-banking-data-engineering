from pyspark import pipelines as dp
import pyspark.sql.functions as F

# Define a Silver staging view containing cleaned and transformed branches data
@dp.view(
    name="stag_branches_silver",
    comment="Transformed branches data ready for CDC upsert"
)

# Apply data quality expectations to validate key branch fields
@dp.expect_or_drop("branch_code is not null", "branch_code IS NOT NULL")
@dp.expect("branch_name is not null","branch_name IS NOT NULL")
@dp.expect("city is not null","city IS NOT NULL")
@dp.expect("state is not null","state IS NOT NULL")

def branches_clean():
    # Read the Bronze branches table using Structured Streaming
    bronze = spark.readStream.table("nexora_catalog.bronze.raw_branches")
    silver = (
        bronze
        # Trim and standardize branch attributes, while applying the required data types
        .withColumn("branch_code", F.trim(F.col("branch_code").cast("string")))
        .withColumn("branch_name", F.initcap(F.trim(F.col("branch_name"))).cast("string"))
        .withColumn("city", F.initcap(F.trim(F.col("city"))).cast("string"))
        .withColumn("state", F.initcap(F.trim(F.col("state"))).cast("string"))
        .withColumn("region", F.initcap(F.trim(F.col("region"))).cast("string"))

        # Classify branches into business-defined branch types based on branch names
        .withColumn("branch_type",
                    F.when(F.col("branch_name").contains("Main"),"Main")
                    .when(F.col("branch_name").contains("Central"),"Central")
                    .when(F.col("branch_name").contains("Corporate"),"Corporate")
                    .when(F.col("branch_name").contains("Tech Park")|
                          F.col("branch_name").contains("Hitech"),"Technology")
                    .when(F.col("branch_name").contains("Diamond")|
                          F.col("branch_name").contains("Marine"),"Specialized")
                    .otherwise("Other"))

        # Preserve ingestion metadata and add the Silver processing timestamp
        .withColumn("file_name", F.col("file_name"))
        .withColumn("ingest_datetime", F.col("ingest_datetime"))
        .withColumn('process_datetime', F.current_timestamp())
    )
    return silver

# Create the final Silver branches table that receives the CDC upserts
dp.create_streaming_table(
    name="nexora_catalog.silver.clean_branches",
    comment="Cleaned and validated branches with CDC upsert capability",
    table_properties={
            "quality": "silver",
            "layer": "silver",
            "delta.enableChangeDataFeed": "true"
        }
    )

# Apply CDC processing using branch_code as the business key.
# SCD Type 2 preserves historical versions of branches records by maintaining both current and expired records.
dp.create_auto_cdc_flow(
    target="nexora_catalog.silver.clean_branches",
    source="stag_branches_silver",
    keys=["branch_code"],
    sequence_by=F.col("ingest_datetime"),
    stored_as_scd_type=2
)