from pyspark import pipelines as dp
import pyspark.sql.functions as F

# Defined the cleaned transactions Silver table with Silver-layer properties
@dp.table(
    name="nexora_catalog.silver.clean_transactions",
    comment="Cleaned and standardized transactions",
    table_properties={
            "quality": "silver",
            "layer": "silver",
            "delta.enableChangeDataFeed": "true"
        }
)

# Apply data quality expectations to validate key transaction fields
@dp.expect("amount is positive", "amount > 0")
@dp.expect_or_drop("txn_id is not null", "txn_id IS NOT NULL")
@dp.expect("account_id is not null", "account_id IS NOT NULL")
@dp.expect("status is valid", "status IN ('SUCCESS','FAILED','PENDING')")

def transactions_clean():

    # Read the Bronze transactions table using Structured Streaming
    bronze = spark.readStream.table("nexora_catalog.bronze.raw_transactions")
    silver = (
        bronze
        # Cast raw columns into their required business data types
        .withColumn('txn_id',F.col('txn_id').cast('int'))
        .withColumn('account_id',F.col('account_id').cast('int'))
        .withColumn('amount',F.col('amount').cast('decimal(10,2)'))
        .withColumn('txn_timestamp',F.col('txn_timestamp').cast('timestamp'))

        # Standardize transaction type, channel, and status values
        .withColumn('txn_type',F.upper(F.trim(F.col('txn_type'))))
        .withColumn('channel',F.upper(F.trim(F.col('channel'))))
        .withColumn('status',F.upper(F.trim(F.col('status'))))

        # Remove records that fail the required transaction validations
        .filter(F.col('account_id').isNotNull())
        .filter(F.col('amount')>0)
        .filter(F.col('status').isin('SUCCESS','FAILED','PENDING'))

        # Derive whether the transaction is inbound or outbound
        .withColumn('transaction_direction',
                    F.when(F.col('txn_type') == 'DEBIT','OUTBOUND')
                    .when(F.col('txn_type') == 'CREDIT','INBOUND')
                    .otherwise('UNKNOWN'))

        # Categorize transactions into business-defined amount segments
        .withColumn('amount_segment',
                    F.when(F.col('amount')<=500,'Micro')
                    .when(F.col('amount')<=2500,'Small')
                    .when(F.col('amount')<=10000,'Medium')
                    .when(F.col('amount')<=50000,'Large')
                    .otherwise("High"))

        # Derive transaction time period and weekday/weekend classification
        .withColumn('transaction_time_period',
                    F.when(F.hour('txn_timestamp').between(6,11),"Morning")
                    .when(F.hour('txn_timestamp').between(12,16),"Afternoon")
                    .when(F.hour('txn_timestamp').between(17,20),"Evening")
                    .otherwise("Night"))

        # Extract year, month, and month name for analysis
        .withColumn('transaction_day_type',
                    F.when(F.dayofweek('txn_timestamp').isin(1,7),"Weekend")
                    .otherwise("Weekday"))
        .withColumn("transaction_year", F.year("txn_timestamp"))
        .withColumn("transaction_month", F.month("txn_timestamp"))
        .withColumn("transaction_month_name", F.date_format("txn_timestamp", "MMMM"))

        # Group transaction channels into broader business categories
        .withColumn("channel_category",
                    F.when(F.col('channel').isin('UPI','NET_BANKING'),'Digital Payment')
                    .when(F.col('channel').isin('IMPS', 'NEFT', 'RTGS'),'Bank Transfer')
                    .when(F.col('channel') == 'POS','Card Payment')
                    .when(F.col('channel') == 'ATM','Cash Withdrawal')
                    .otherwise("Other")
                    )

        # Create a flag to identify successful transactions
        .withColumn("is_successfull",F.when(F.col("status") == "SUCCESS", 1).otherwise(0))

        # Remove duplicate transactions using txn_id as the business key
        .dropDuplicates(["txn_id"])

        # Preserve ingestion metadata and add the Silver processing timestamp
        .withColumn('file_name', F.col('file_name'))
        .withColumn('ingest_datetime', F.col('ingest_datetime'))
        .withColumn('process_datetime', F.current_timestamp())
    )

    return silver