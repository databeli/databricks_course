# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Overview
# MAGIC This notebook demonstrates how to orchestrate and modularize code in Databricks notebooks using `%run` and `dbutils.notebook.run`. It includes examples of passing parameters, handling outputs, and managing exceptions effectively.

# COMMAND ----------

# MAGIC %md
# MAGIC # %run

# COMMAND ----------

# MAGIC %md
# MAGIC ## %run wihtout parameters

# COMMAND ----------

# MAGIC %run "./02_Notebook_02_Utils_Without_Params"

# COMMAND ----------

print("Test Variable=",test_variable)
print("Area of Circle=",area_of_circle(5))

# COMMAND ----------

# MAGIC %run "/Workspace/Users/databeli14@gmail.com/PySpark and Databricks Complete Course/03 Databricks/02_Notebook_Run_from_Notebook/02_Notebook_02_Utils_Without_Params"

# COMMAND ----------

# MAGIC %md
# MAGIC ## %run wih parameters

# COMMAND ----------

# MAGIC %run "./02_Notebook_03_Utils_With_Params"  $name="Narender" $radius="10"

# COMMAND ----------

# MAGIC %md
# MAGIC # DButils

# COMMAND ----------

# MAGIC %md
# MAGIC ## DButils without Parameters

# COMMAND ----------

dbutils.notebook.run("./02_Notebook_03_Utils_With_Params",60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DButils with Parameters

# COMMAND ----------

dbutils.notebook.run("./02_Notebook_03_Utils_With_Params",60,{"name":"Narender","radius":"50"})

# COMMAND ----------

# MAGIC %md
# MAGIC ## DButils receive Output

# COMMAND ----------

circle_area=dbutils.notebook.run("./02_Notebook_04_Utils_With_Output",60,{"name":"Narender","radius":"50"})
print("Circle Area:",circle_area)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DButils Handle Exceptions

# COMMAND ----------

try:
    output = dbutils.notebook.run("./02_Notebook_05_Utils_With_Output_and_Exception",60,{"name":"Narender","radius":"50"})
    print(output)
except Exception as e:
    print("Child notebook failed:", e)