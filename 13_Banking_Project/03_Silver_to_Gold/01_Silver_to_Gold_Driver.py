# Databricks notebook source
import json
from datetime import datetime

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("table_metadata", "")

run_id = dbutils.widgets.get("run_id")
table_metadata = dbutils.widgets.get("table_metadata")

if not run_id or not table_metadata:
    raise ValueError("run_id and table_metadata are required")

table_metadata = json.loads(table_metadata.replace("'", '"'))

table_id = table_metadata["table_id"]
table_name = table_metadata["table_name"]

start_time = datetime.utcnow()

print("Run ID:", run_id)
print("Table ID:", table_id)
print("Table Name:", table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists banking.gold;

# COMMAND ----------

entry_exists = spark.sql(f"""
    SELECT 1
    FROM banking.metadata.pipeline_runs
    WHERE run_id = {run_id}
    AND table_id = {table_id}
""").count() > 0

if entry_exists:
    
    spark.sql(f"""
        UPDATE banking.metadata.pipeline_runs
        SET
            layer = 'Gold',
            start_time = TIMESTAMP('{start_time}'),
            end_time = NULL,
            status = 'INPROGRESS',
            number_of_records = NULL,
            error_message = NULL
        WHERE run_id = {run_id}
        AND table_id = {table_id}
    """)

else:

    spark.sql(f"""
        INSERT INTO banking.metadata.pipeline_runs
        VALUES (
            {run_id},
            {table_id},
            'Gold',
            TIMESTAMP('{start_time}'),
            NULL,
            'INPROGRESS',
            NULL,
            NULL
        )
    """)

print("Audit entry created / updated")

# COMMAND ----------

notebook_path = f"gold_transformations/{table_name}"

print("Notebook to execute:", notebook_path)

# COMMAND ----------

status = "SUCCESS"
error_message = None
records = None

try:

    result = dbutils.notebook.run(
        notebook_path,
        timeout_seconds=0
    )

    # Expect the notebook to return record count
    if result:
        records = int(result)

    print("Notebook completed successfully")
    print("Records:", records)

except Exception as e:

    status = "FAILED"
    error_message = str(e)

    print("Notebook failed")
    print(error_message)

# COMMAND ----------

end_time = datetime.utcnow()

spark.sql(f"""
    UPDATE banking.metadata.pipeline_runs
    SET
        end_time = TIMESTAMP('{end_time}'),
        status = '{status}',
        number_of_records = {records if records else 'NULL'},
        error_message = {f"'{error_message}'" if error_message else 'NULL'}
    WHERE run_id = {run_id}
    AND table_id = {table_id}
""")

print("Audit table updated")

# COMMAND ----------

if status == "FAILED":
    raise Exception(error_message)