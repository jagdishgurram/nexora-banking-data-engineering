CREATE MATERIALIZED VIEW nexora_catalog.gold.transaction_summary AS

SELECT
    t.transaction_year,
    t.transaction_month,
    t.txn_type AS transaction_type,
    t.channel_category,

    COUNT(t.txn_id) AS transaction_count,
    SUM(CASE WHEN t.is_successfull = 1 THEN 1 ELSE 0 END) AS successful_transaction_count,
    SUM(CASE WHEN t.is_successfull = 0 THEN 1 ELSE 0 END) AS failed_transactions_count,
    
    SUM(t.amount) AS total_transaction_amount,
    AVG(t.amount) AS average_transaction_amount

FROM nexora_catalog.silver.clean_transactions t

GROUP BY 
    t.transaction_year,
    t.transaction_month,
    t.txn_type,
    t.channel_category;