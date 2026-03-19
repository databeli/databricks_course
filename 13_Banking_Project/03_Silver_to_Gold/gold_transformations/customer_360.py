# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE banking.gold.customer_360 AS
# MAGIC
# MAGIC WITH account_agg AS (
# MAGIC     SELECT
# MAGIC         customer_id,
# MAGIC         COUNT(account_id) AS total_accounts,
# MAGIC         SUM(balance) AS total_balance
# MAGIC     FROM banking.silver.accounts
# MAGIC     GROUP BY customer_id
# MAGIC ),
# MAGIC
# MAGIC txn_agg AS (
# MAGIC     SELECT
# MAGIC         a.customer_id,
# MAGIC         COUNT(t.txn_id) AS total_transactions,
# MAGIC         SUM(t.amount) AS total_transaction_amount
# MAGIC     FROM banking.silver.transactions t
# MAGIC     JOIN banking.silver.accounts a
# MAGIC         ON t.account_id = a.account_id
# MAGIC     GROUP BY a.customer_id
# MAGIC ),
# MAGIC
# MAGIC credit_latest AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT *,
# MAGIC                ROW_NUMBER() OVER (
# MAGIC                    PARTITION BY customer_id
# MAGIC                    ORDER BY bureau_pull_date DESC
# MAGIC                ) AS rn
# MAGIC         FROM banking.silver.credit_bureau_reports
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     c.customer_id,
# MAGIC     CONCAT(c.first_name,' ',c.last_name) AS customer_name,
# MAGIC     b.branch_name,
# MAGIC     COALESCE(a.total_accounts,0) AS total_accounts,
# MAGIC     COALESCE(a.total_balance,0) AS total_balance,
# MAGIC     COALESCE(t.total_transactions,0) AS total_transactions,
# MAGIC     COALESCE(t.total_transaction_amount,0) AS total_transaction_amount,
# MAGIC     cr.credit_score,
# MAGIC     cr.risk_grade,
# MAGIC     cr.external_active_loans,
# MAGIC     cr.external_overdue_amount,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN a.total_balance >= 500000 THEN 'HIGH_VALUE'
# MAGIC         WHEN a.total_balance >= 100000 THEN 'MEDIUM_VALUE'
# MAGIC         ELSE 'LOW_VALUE'
# MAGIC     END AS customer_segment
# MAGIC
# MAGIC FROM banking.silver.customers c
# MAGIC
# MAGIC LEFT JOIN account_agg a
# MAGIC     ON c.customer_id = a.customer_id
# MAGIC
# MAGIC LEFT JOIN txn_agg t
# MAGIC     ON c.customer_id = t.customer_id
# MAGIC
# MAGIC LEFT JOIN banking.silver.branches b
# MAGIC     ON c.branch_code = b.branch_code
# MAGIC
# MAGIC LEFT JOIN credit_latest cr
# MAGIC     ON c.customer_id = cr.customer_id

# COMMAND ----------

count = spark.sql("""
SELECT COUNT(*) AS cnt
FROM banking.gold.customer_360
""").collect()[0]["cnt"]

dbutils.notebook.exit(str(count))