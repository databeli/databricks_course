# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE banking.gold.risk_customer_summary AS
# MAGIC
# MAGIC SELECT
# MAGIC risk_grade,
# MAGIC COUNT(customer_id) AS total_customers,
# MAGIC AVG(credit_score) AS avg_credit_score,
# MAGIC SUM(external_active_loans) AS total_external_loans,
# MAGIC SUM(external_overdue_amount) AS total_overdue_amount
# MAGIC
# MAGIC FROM banking.silver.credit_bureau_reports
# MAGIC
# MAGIC GROUP BY risk_grade

# COMMAND ----------

count = spark.sql("""
SELECT COUNT(*) AS cnt
FROM banking.gold.risk_customer_summary
""").collect()[0]["cnt"]

dbutils.notebook.exit(str(count))