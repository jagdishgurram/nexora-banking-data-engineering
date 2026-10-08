from pyspark import pipelines as dp
import pyspark.sql.functions as F

# Define a Silver staging view containing cleaned and transformed customers data
@dp.view(
    name="stag_customers_silver",
    comment="Transformed customers data ready for CDC upsert"
)

# Apply data quality expectations to validate key customer fields
@dp.expect_or_drop("customer_id is not null", "customer_id IS NOT NULL")
@dp.expect("branch_code is not null", "branch_code IS NOT NULL")
@dp.expect("kyc_status is valid", "kyc_status IN ('VERIFIED','PENDING','REJECTED')")
@dp.expect("PAN format is valid", "is_pan_valid = 1")


def customers_clean():
    # Read the Bronze customers table using Structured Streaming
    bronze = spark.readStream.table("nexora_catalog.bronze.raw_customers")
    silver = (
        bronze
        # Cast raw columns into their required business data types
        .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))).cast("string"))
        .withColumn("last_name", F.initcap(F.trim(F.col("last_name"))).cast("string"))
        .withColumn("customer_id", F.col("customer_id").cast("string"))
        .withColumn("date_of_birth", F.to_date("date_of_birth").cast("date"))
        .withColumn("pan_number", F.upper(F.trim("pan_number")).cast("string"))
        .withColumn("kyc_status", F.upper(F.trim("kyc_status")).cast("string"))
        .withColumn("branch_code", F.upper(F.trim("branch_code")).cast("string"))
        .withColumn("email", F.lower(F.trim("email")).cast("string"))

        # Standardize phone number, email and customers name
        .withColumn('phone_number',F.regexp_replace(F.col('phone_number'),"[^0-9]","").cast("string"))
        .withColumn('email',F.regexp_replace(F.col('email'),r"[^a-z0-9._@-]",""))
        .withColumn('full_name',F.concat_ws(' ','first_name','last_name'))

        # Validate then Mask sensitive PAN and phone number information before storing in Silver.
        .withColumn('is_pan_valid',F.when(F.col('pan_number').rlike(r"^[A-Z]{5}[0-9]{4}[A-Z]$"),1).otherwise(0))
        .withColumn('pan_number',F.regexp_replace("pan_number", r"^([A-Z]{3})[A-Z0-9]{6}([A-Z])$", r"$1******$2"))
        .withColumn('phone_number',F.regexp_replace("phone_number", r"^\d{6}(\d{4})$", r"******$1"))

        # Calculate customer age and classify customers into defined age groups.
        .withColumn('customer_age', F.floor(F.months_between(F.current_date(),F.col('date_of_birth')) / 12))
        .withColumn('age_group',
                    F.when(F.col('customer_age') < 18,'Minor')
                    .when(F.col('customer_age') <= 30,'Young Adult')
                    .when(F.col('customer_age') <= 50,'Adult')
                    .when(F.col('customer_age') <= 65,'Mature')
                    .otherwise('Senior'))

        # Create an indicator for currently kyc verified customers
        .withColumn("is_kyc_verified",F.when(F.col("kyc_status") == "VERIFIED", 1).otherwise(0))

        # Preserve ingestion metadata and add the Silver processing timestamp
        .withColumn("file_name", F.col("file_name"))
        .withColumn("ingest_datetime", F.col("ingest_datetime"))
        .withColumn('process_datetime', F.current_timestamp())
    )
    return silver

# Create the final Silver customers table that receives the CDC upserts
dp.create_streaming_table(
    name="nexora_catalog.silver.clean_customers",
    comment="Cleaned and validated customers with CDC upsert capability",
    table_properties={
            "quality": "silver",
            "layer": "silver",
            "delta.enableChangeDataFeed": "true"
        }
    )

# Apply CDC processing using customer_id as the business key.
# SCD Type 2 preserves historical versions of customer records by maintaining both current and expired records.
dp.create_auto_cdc_flow(
    target="nexora_catalog.silver.clean_customers",
    source="stag_customers_silver",
    keys=["customer_id"],
    sequence_by=F.col("ingest_datetime"),
    stored_as_scd_type=2
)

