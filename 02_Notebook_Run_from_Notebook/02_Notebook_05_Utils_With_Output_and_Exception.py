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


# COMMAND ----------

status=""

# COMMAND ----------

try:
    def area_of_circle(r):
        return 3.14159 * r * r
    circle_area = area_of_circle(radius)
    status = "SUCCESS"
except Exception as e:
    circle_area = None
    status = "ERROR"
    error_message = str(e)

# COMMAND ----------

print(f"Helper notebook executed with name={name}, radius={radius}")

# COMMAND ----------

import json
output=None
if status == "ERROR":
    output = {
        "status": "ERROR",
        "error_message": error_message
    }
else:
    output = {
    "circle_area": circle_area,
    "status": "SUCCESS"
    }
dbutils.notebook.exit(json.dumps(output))
