# Databricks notebook source
df = spark.read.table("training.jobs_demo.list_of_tables")
table_list = df.collect()
table_list_dict=[row.asDict() for row in table_list]

# COMMAND ----------

import json
json_array_str = json.dumps([row.asDict().get("table_name") for row in table_list])
print(json_array_str)

# COMMAND ----------

dbutils.jobs.taskValues.set(
    key="tables_list",
    value=json_array_str
)