# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE banking.gold.daily_bank_kpi AS
# MAGIC
# MAGIC WITH txn_daily AS (
# MAGIC     SELECT
# MAGIC         DATE(txn_timestamp) AS txn_date,
# MAGIC         COUNT(txn_id) AS total_transactions,
# MAGIC         SUM(amount) AS total_transaction_amount
# MAGIC     FROM banking.silver.transactions
# MAGIC     GROUP BY DATE(txn_timestamp)
# MAGIC ),
# MAGIC
# MAGIC customer_metrics AS (
# MAGIC     SELECT
# MAGIC         COUNT(DISTINCT customer_id) AS total_customers
# MAGIC     FROM banking.silver.customers
# MAGIC ),
# MAGIC
# MAGIC account_metrics AS (
# MAGIC     SELECT
# MAGIC         COUNT(account_id) AS total_accounts,
# MAGIC         SUM(balance) AS total_balance
# MAGIC     FROM banking.silver.accounts
# MAGIC ),
# MAGIC
# MAGIC credit_metrics AS (
# MAGIC     SELECT
# MAGIC         AVG(credit_score) AS avg_credit_score,
# MAGIC         SUM(
# MAGIC             CASE WHEN risk_grade='HIGH'
# MAGIC             THEN 1 ELSE 0 END
# MAGIC         ) AS high_risk_customers
# MAGIC     FROM banking.silver.credit_bureau_reports
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC t.txn_date,
# MAGIC cm.total_customers,
# MAGIC am.total_accounts,
# MAGIC am.total_balance,
# MAGIC t.total_transactions,
# MAGIC t.total_transaction_amount,
# MAGIC cr.avg_credit_score,
# MAGIC cr.high_risk_customers
# MAGIC
# MAGIC FROM txn_daily t
# MAGIC CROSS JOIN customer_metrics cm
# MAGIC CROSS JOIN account_metrics am
# MAGIC CROSS JOIN credit_metrics cr

# COMMAND ----------

count = spark.sql("""
SELECT COUNT(*) AS cnt
FROM banking.gold.daily_bank_kpi
""").collect()[0]["cnt"]

dbutils.notebook.exit(str(count))