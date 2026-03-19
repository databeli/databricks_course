# Databricks notebook source
# ==========================================
# 1️⃣ Create Widget
# ==========================================

dbutils.widgets.text("source_system", "")
source_system = dbutils.widgets.get("source_system")

print(f"Source System Passed: {source_system}")

if not source_system:
    raise ValueError("source_system widget cannot be empty")


# ==========================================
# 2️⃣ Read Metadata Tables
# ==========================================

tables_df = spark.table("banking.metadata.tables")

filtered_df = (
    tables_df
    .filter(f"active_flag = true AND lower(source_system) = lower('{source_system}')")
    .orderBy("load_order")
)

display(filtered_df)


# ==========================================
# 3️⃣ Convert to List of Dictionaries
# ==========================================

rows = filtered_df.collect()

tables_list = [
    {
        "table_id": str(row.table_id) if row.table_id is not None else "",
        "table_name": str(row.table_name) if row.table_name is not None else "",
        "source_system": str(row.source_system) if row.source_system is not None else "",
        "source_schema": str(row.source_schema) if row.source_schema is not None else "",
        "source_table": str(row.source_table) if row.source_table is not None else "",
        "source_path": str(row.source_path) if row.source_path is not None else "",
        "target_layer": str(row.target_layer) if row.target_layer is not None else "",
        "bronze_schema": str(row.bronze_schema) if row.bronze_schema is not None else "",
        "silver_schema": str(row.silver_schema) if row.silver_schema is not None else "",
        "gold_schema": str(row.gold_schema) if row.gold_schema is not None else "",
        "active_flag": str(row.active_flag) if row.active_flag is not None else "",
        "load_order": str(row.load_order) if row.load_order is not None else "",
        "created_at": str(row.created_at) if row.created_at is not None else ""
    }
    for row in rows
]

print("Tables to Process (Full Metadata):")
print(tables_list)


# ==========================================
# 4️⃣ Set Databricks Task Value
# ==========================================

dbutils.jobs.taskValues.set(
    key="tables_metadata",
    value=tables_list
)

print("Task value 'tables_metadata' has been set.")
