# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE banking.gold.branch_performance AS
# MAGIC
# MAGIC WITH customer_branch AS (
# MAGIC     SELECT
# MAGIC         c.customer_id,
# MAGIC         c.branch_code
# MAGIC     FROM banking.silver.customers c
# MAGIC ),
# MAGIC
# MAGIC account_agg AS (
# MAGIC     SELECT
# MAGIC         a.customer_id,
# MAGIC         COUNT(a.account_id) AS total_accounts,
# MAGIC         SUM(a.balance) AS total_balance
# MAGIC     FROM banking.silver.accounts a
# MAGIC     GROUP BY a.customer_id
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
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     b.branch_code,
# MAGIC     b.branch_name,
# MAGIC     COUNT(DISTINCT cb.customer_id) AS total_customers,
# MAGIC     SUM(a.total_accounts) AS total_accounts,
# MAGIC     SUM(a.total_balance) AS total_deposits,
# MAGIC     SUM(t.total_transactions) AS total_transactions,
# MAGIC     SUM(t.total_transaction_amount) AS total_transaction_amount
# MAGIC
# MAGIC FROM banking.silver.branches b
# MAGIC
# MAGIC LEFT JOIN customer_branch cb
# MAGIC     ON b.branch_code = cb.branch_code
# MAGIC
# MAGIC LEFT JOIN account_agg a
# MAGIC     ON cb.customer_id = a.customer_id
# MAGIC
# MAGIC LEFT JOIN txn_agg t
# MAGIC     ON cb.customer_id = t.customer_id
# MAGIC
# MAGIC GROUP BY
# MAGIC b.branch_code,
# MAGIC b.branch_name

# COMMAND ----------

count = spark.sql("""
SELECT COUNT(*) AS cnt
FROM banking.gold.branch_performance
""").collect()[0]["cnt"]

dbutils.notebook.exit(str(count))