CREATE MATERIALIZED VIEW nexora_catalog.gold.branch_summary AS

WITH transactions_summary AS (
    SELECT 
        t.account_id,
        SUM(t.amount) AS total_transaction_amount,
        COUNT(t.txn_id) AS transaction_count
    FROM nexora_catalog.silver.clean_transactions t
    GROUP BY t.account_id        
)

SELECT 
    b.branch_code,
    b.branch_name,
    b.city,
    b.region,

    COUNT(DISTINCT a.account_id) AS account_count,
    SUM(CASE WHEN a.is_active = 1 THEN 1 ELSE 0 END) AS active_account_count,
    COUNT(DISTINCT a.customer_id) AS customer_count,
    SUM(a.balance) AS total_balance,
    COALESCE(SUM(t.total_transaction_amount), 0) AS total_transaction_amount,
    COALESCE(SUM(t.transaction_count), 0) AS transaction_count
    
FROM nexora_catalog.silver.clean_branches b

LEFT JOIN nexora_catalog.silver.clean_accounts a
    ON b.branch_code = a.branch_code

LEFT JOIN transactions_summary t
    ON t.account_id = a.account_id
    
GROUP BY
    b.branch_code,
    b.branch_name,
    b.city,
    b.region

ORDER BY b.branch_code;
