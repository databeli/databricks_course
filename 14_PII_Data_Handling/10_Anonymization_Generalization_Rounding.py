# Databricks notebook source
# DBTITLE 1,Read Source Table
# Read the source table
df_employees = spark.table("pii_demo.pii_source.employees")

# Display the original data
print("Original Data:")
display(df_employees)

# COMMAND ----------

# DBTITLE 1,Apply Generalization - Round Salary
from pyspark.sql.functions import col, floor, round as spark_round

# Apply generalization by rounding salary to nearest 10,000
# This protects individual privacy while maintaining data utility
df_rounded = df_employees.withColumn(
    "salary_rounded",
    (floor(col("salary") / 10000) * 10000).cast("bigint")
)

# Replace the original salary column with the rounded one
df_anonymized = df_rounded.withColumn("salary", col("salary_rounded")).drop("salary_rounded")

print("Anonymized Data with Rounded Salaries:")
display(df_anonymized)

# COMMAND ----------

# DBTITLE 1,Write to Target Table
# Write the anonymized data to the target table
df_anonymized.write \
    .mode("overwrite") \
    .saveAsTable("pii_demo.pii_target.employees_rounded")

print("✓ Data successfully written to pii_demo.pii_target.employees_rounded")

# Verify the written data
df_verify = spark.table("pii_demo.pii_target.employees_rounded")
print(f"\nTotal records written: {df_verify.count()}")
display(df_verify)