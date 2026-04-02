# Databricks notebook source
# DBTITLE 1,Apply Categorical Generalization on Job Title
from pyspark.sql import functions as F

# Read source table
df = spark.table("pii_demo.pii_source.employees")

# Define categorical generalization mapping for job titles
# Specific job titles will be grouped into broader categories
job_category_mapping = {
    "Software Engineer": "Engineering",
    "Data Scientist": "Data & Analytics",
    "Product Manager": "Management",
    "Sales Executive": "Sales & Business",
    "HR Manager": "Management"
}

# Create a mapping expression using CASE WHEN
mapping_expr = F.when(F.col("job_title") == "Software Engineer", "Engineering") \
    .when(F.col("job_title") == "Data Scientist", "Data & Analytics") \
    .when(F.col("job_title") == "Product Manager", "Management") \
    .when(F.col("job_title") == "Sales Executive", "Sales & Business") \
    .when(F.col("job_title") == "HR Manager", "Management") \
    .otherwise("Other")

# Apply categorical generalization
df_generalized = df.withColumn("job_title_generalized", mapping_expr) \
    .drop("job_title") \
    .withColumnRenamed("job_title_generalized", "job_title")

# Write to target table
df_generalized.write.mode("overwrite").saveAsTable("pii_demo.pii_target.employees_categorical_generalized")

print("Categorical generalization applied successfully!")
print(f"Total records processed: {df_generalized.count()}")

# Show sample of generalized data
display(df_generalized.limit(10))