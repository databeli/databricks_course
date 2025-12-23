# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists training.jobs_demo.list_of_tables

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists training.jobs_demo.list_of_tables (table_name string)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into training.jobs_demo.list_of_tables 
# MAGIC values ('sales_customers'), 
# MAGIC ('sales_transactions')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from training.jobs_demo.list_of_tables