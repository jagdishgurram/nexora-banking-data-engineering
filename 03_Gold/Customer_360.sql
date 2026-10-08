CREATE MATERIALIZED VIEW nexora_catalog.gold.customer_360 AS

WITH account_summary AS (
    SELECT
        customer_id,
        COUNT(DISTINCT account_id) AS account_count,
        SUM(balance) AS total_balance,
        SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active_account_count
    FROM nexora_catalog.silver.clean_accounts
    GROUP BY customer_id
),

transactions_summary AS (
    SELECT
        a.customer_id,
        COUNT(t.txn_id) AS total_transactions,
        SUM(CASE WHEN t.is_successfull = 1 THEN 1 ELSE 0 END) AS successful_transactions,
        SUM(CASE WHEN t.is_successfull = 0 THEN 1 ELSE 0 END) AS failed_transactions,
        SUM(t.amount) AS total_transaction_amount,
        MAX(t.txn_timestamp) AS last_transaction_date
    FROM nexora_catalog.silver.clean_transactions t
    JOIN nexora_catalog.silver.clean_accounts a
        ON t.account_id = a.account_id
    GROUP BY a.customer_id
)

SELECT
    c.customer_id,
    c.full_name,
    c.customer_age,
    c.kyc_status,

    COALESCE(a.account_count, 0) AS account_count,
    COALESCE(a.total_balance, 0) AS total_balance,
    COALESCE(a.active_account_count, 0) AS active_account_count,

    COALESCE(t.total_transactions, 0) AS total_transactions,
    COALESCE(t.successful_transactions, 0) AS successful_transactions,
    COALESCE(t.failed_transactions, 0) AS failed_transactions,
    COALESCE(t.total_transaction_amount, 0) AS total_transaction_amount,
    t.last_transaction_date

FROM nexora_catalog.silver.clean_customers c

LEFT JOIN account_summary a
    ON a.customer_id = c.customer_id

LEFT JOIN transactions_summary t
    ON t.customer_id = c.customer_id;