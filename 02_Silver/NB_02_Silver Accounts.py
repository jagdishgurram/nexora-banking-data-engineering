from pyspark import pipelines as dp
import pyspark.sql.functions as F

# Define a Silver staging view containing cleaned and transformed account data
@dp.view(
    name="stag_accounts_silver",
    comment="Transformed accounts data ready for CDC upsert"
)

# Apply data quality expectations to validate key account fields
@dp.expect_or_drop("account_id is not null", "account_id IS NOT NULL")
@dp.expect("status is valid", "status IN ('ACTIVE','DORMANT','CLOSED')")

def accounts_clean():
    # Read the Bronze accounts table using Structured Streaming
    bronze = spark.readStream.table("nexora_catalog.bronze.raw_accounts")
    silver = (
        bronze
        # Cast raw columns into their required business data types
        .withColumn("account_id", F.col("account_id").cast("string"))
        .withColumn("customer_id", F.col("customer_id").cast("string"))
        .withColumn("balance", F.col("balance").cast("decimal(18,2)"))
        .withColumn("opened_date", F.col("opened_date").cast("date"))

        # Standardize account type, currency, branch code and status values
        .withColumn("account_type", F.initcap(F.trim(F.col("account_type"))))
        .withColumn("currency", F.upper(F.trim(F.col("currency"))))
        .withColumn("branch_code", F.upper(F.trim(F.col("branch_code"))))
        .withColumn("status", F.upper(F.trim(F.col("status"))))

        # Categorize accounts based on their business account type
        .withColumn("account_category",
                    F.when(F.col("account_type").isin("Savings","Salary"),"Retail")
                    .when(F.col("account_type")=="Current","Business")
                    .when(F.col("account_type")=="Fixed Deposit","Investment")
                    .otherwise("Other")
                    )

        # Classify accounts into balance segments based on balance amount
        .withColumn("balance_segment",
                    F.when(F.col("balance") <= 50000,"Low")
                    .when(F.col("balance") < 250000,"Medium")
                    .when(F.col("balance") < 500000,"High")
                    .otherwise("Premium")
                    )

        # Calculate the account age in completed years from the opening date
        .withColumn("account_age_years",
                    F.floor(F.months_between(F.current_date(), F.col("opened_date")) / 12))

        # Apply basic business validations before loading into the Silver table
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("balance") >= 0)
        .filter(F.col("status").isin("ACTIVE", "DORMANT", "CLOSED"))

        # Create an indicator for currently active accounts
        .withColumn("is_active",F.when(F.col("status") == "ACTIVE", 1).otherwise(0))

        # Preserve ingestion metadata and add the Silver processing timestamp
        .withColumn("file_name", F.col("file_name"))
        .withColumn("ingest_datetime", F.col("ingest_datetime"))
        .withColumn('process_datetime', F.current_timestamp())
    )
    return silver

# Create the final Silver accounts table that receives the CDC upserts
dp.create_streaming_table(
    name="nexora_catalog.silver.clean_accounts",
    comment="Cleaned and validated Accounts with CDC upsert capability",
    table_properties={
            "quality": "silver",
            "layer": "silver",
            "delta.enableChangeDataFeed": "true"
        }
    )

# Apply CDC processing using account_id as the business key.
# SCD Type 1 keeps the latest version of each account record.
dp.create_auto_cdc_flow(
    target="nexora_catalog.silver.clean_accounts",
    source="stag_accounts_silver",
    keys=["account_id"],
    sequence_by=F.col("ingest_datetime"),
    stored_as_scd_type=1
)

