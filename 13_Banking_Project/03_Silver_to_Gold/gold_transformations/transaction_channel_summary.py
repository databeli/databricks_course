# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE banking.gold.transaction_channel_summary AS
# MAGIC
# MAGIC SELECT
# MAGIC DATE(t.txn_timestamp) AS txn_date,
# MAGIC pg.gateway_name,
# MAGIC pg.device_type,
# MAGIC
# MAGIC COUNT(*) AS total_transactions,
# MAGIC
# MAGIC SUM(
# MAGIC     CASE WHEN pg.gateway_status = 'SUCCESS'
# MAGIC     THEN 1 ELSE 0 END
# MAGIC ) AS successful_transactions,
# MAGIC
# MAGIC SUM(
# MAGIC     CASE WHEN pg.gateway_status = 'FAILED'
# MAGIC     THEN 1 ELSE 0 END
# MAGIC ) AS failed_transactions,
# MAGIC
# MAGIC AVG(pg.processing_time_ms) AS avg_processing_time_ms
# MAGIC
# MAGIC FROM banking.silver.transactions t
# MAGIC
# MAGIC JOIN banking.silver.payment_gateway_logs pg
# MAGIC     ON t.txn_id = pg.txn_id
# MAGIC
# MAGIC GROUP BY
# MAGIC txn_date,
# MAGIC pg.gateway_name,
# MAGIC pg.device_type

# COMMAND ----------

count = spark.sql("""
SELECT COUNT(*) AS cnt
FROM banking.gold.transaction_channel_summary
""").collect()[0]["cnt"]

dbutils.notebook.exit(str(count))