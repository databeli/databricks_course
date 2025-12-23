# Databricks notebook source
# MAGIC %md
# MAGIC ## Calculate country wise transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table training.jobs_demo.transaction_summary as
# MAGIC select country,count(*) as number_of_transactions from (
# MAGIC select * from samples.bakehouse.sales_customers customers join samples.bakehouse.sales_transactions transactions on customers.customerID = transactions.customerID
# MAGIC ) group by country

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from training.jobs_demo.transaction_summary