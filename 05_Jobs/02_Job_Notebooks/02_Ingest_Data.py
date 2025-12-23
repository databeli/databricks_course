# Databricks notebook source
# MAGIC %md
# MAGIC ## Widget to take table name as input

# COMMAND ----------

dbutils.widgets.text("table_name","sales_customers")
table_name = dbutils.widgets.get("table_name")
print(table_name)

# COMMAND ----------

# if table_name == "sales_customers":
#   raise Exception("sales_customers has been failed due to some reason")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest table from sample schema to our own schema

# COMMAND ----------

query=f"create or replace table training.jobs_demo.{table_name} as select * from samples.bakehouse.{table_name}"
spark.sql(query)