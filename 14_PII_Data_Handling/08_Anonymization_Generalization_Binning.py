# Databricks notebook source
# DBTITLE 1,Apply Binning on Date of Birth
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, year, months_between, floor, current_date

# COMMAND ----------

# DBTITLE 1,Read Source Table
# Read source table
df = spark.read.table("pii_demo.pii_source.customers")

# Display sample records
display(df.limit(5))

# COMMAND ----------

# DBTITLE 1,Calculate Age and Apply Binning
# Calculate age from date_of_birth
df_with_age = df.withColumn(
    "age",
    floor(months_between(current_date(), col("date_of_birth")) / 12)
)

# Apply binning to create age ranges
df_binned = df_with_age.withColumn(
    "age_range",
    when(col("age") < 18, "Under 18")
    .when((col("age") >= 18) & (col("age") <= 25), "18-25")
    .when((col("age") >= 26) & (col("age") <= 35), "26-35")
    .when((col("age") >= 36) & (col("age") <= 45), "36-45")
    .when((col("age") >= 46) & (col("age") <= 55), "46-55")
    .when((col("age") >= 56) & (col("age") <= 65), "56-65")
    .otherwise("66+")
)

# Select all columns except the intermediate age column and date_of_birth
# Replace date_of_birth with age_range
df_final = df_binned.select(
    "customer_id",
    "full_name",
    "email",
    "phone",
    "ssn",
    "age_range",  # Binned date_of_birth
    "address",
    "city",
    "country"
)

print("Age binning applied successfully")

# Show age distribution
print("\nAge Range Distribution:")
df_final.groupBy("age_range").count().orderBy("age_range").display()

# COMMAND ----------

# DBTITLE 1,Write to Target Table
# Write to target table
df_final.write.mode("overwrite").saveAsTable("pii_demo.pii_target.customers_binning")

print("✓ Data successfully written to pii_demo.pii_target.customers_binning")
print(f"✓ Total records processed: {df_final.count()}")

# COMMAND ----------

# DBTITLE 1,Verify Results
# Read the target table to verify
df_result = spark.read.table("pii_demo.pii_target.customers_binning")

print("\nSample binned data:")
display(df_result.limit(10))