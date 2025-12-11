# Databricks notebook source
# helper_notebook

dbutils.widgets.text("name", "default_name")
dbutils.widgets.text("radius", "5")

# COMMAND ----------

name = dbutils.widgets.get("name")
radius = float(dbutils.widgets.get("radius"))

# COMMAND ----------

def greet_user():
    return f"Hello, {name}!"

def area_of_circle(r):
    return 3.14159 * r * r

circle_area = area_of_circle(radius)

# COMMAND ----------

print(f"Helper notebook executed with name={name}, radius={radius}")

# COMMAND ----------

dbutils.notebook.exit(circle_area)

# COMMAND ----------

# import json

# output = {
#     "circle_area": circle_area,
#     "status": "SUCCESS"
# }
# dbutils.notebook.exit(json.dumps(output))
