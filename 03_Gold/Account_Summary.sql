CREATE MATERIALIZED VIEW nexora_catalog.gold.account_summary AS

SELECT
    a.account_type,
    a.account_category,
    a.balance_segment,
    b.region,

    COUNT(DISTINCT a.account_id) AS account_count,
    COUNT(DISTINCT a.customer_id) AS customer_count,

    CAST(SUM(a.balance)AS DECIMAL(18,2)) AS total_balance,
    CAST(AVG(a.balance)AS DECIMAL(18,2)) AS average_balance,

    COUNT(CASE WHEN a.is_active = 1 THEN 1 END) AS active_account_count,

    CAST(AVG(a.account_age_years) AS DECIMAL(18,2)) AS average_account_age

FROM nexora_catalog.silver.clean_accounts a

LEFT JOIN nexora_catalog.silver.clean_branches b
    ON a.branch_code = b.branch_code

GROUP BY
    a.account_type,
    a.account_category,
    a.balance_segment,
    b.region;