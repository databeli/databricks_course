# Databricks notebook source
# DBTITLE 1,Read Source Table
# Read the source table containing PII data
source_table = "pii_demo.pii_source.customers"

df_source = spark.table(source_table)

# Display sample data
print("\nSample data (first 3 rows):")
display(df_source.limit(3))

# COMMAND ----------

# DBTITLE 1,Apply Hashing Functions to PII Columns
# Apply different hashing functions to each PII column using built-in PySpark functions
# Keep non-PII columns (customer_id, country) unchanged
from pyspark.sql import functions as F
df_hashed = df_source.select(
    # Non-PII column - keep as is
    F.col("customer_id"),
    
    # PII columns - apply different hashing algorithms using built-in functions
    # 1. full_name: SHA-256 hashing
    F.sha2(F.col("full_name"), 256).alias("full_name"),
    
    # 2. email: MD5 hashing
    F.md5(F.col("email")).alias("email"),
    
    # 3. phone: SHA-1 hashing (cast bigint to string first)
    F.sha1(F.col("phone").cast("string")).alias("phone"),
    
    # 4. ssn: SHA-512 hashing (most secure for sensitive data)
    F.sha2(F.col("ssn"), 512).alias("ssn"),
    
    # 5. date_of_birth: SHA-384 hashing (cast date to string first)
    F.sha2(F.col("date_of_birth").cast("string"), 384).alias("date_of_birth"),
    
    # 6. address: SHA-224 hashing
    F.sha2(F.col("address"), 224).alias("address"),
    
    # 7. city: MD5 hashing
    F.md5(F.col("city")).alias("city"),
    
    # Non-PII column - keep as is
    F.col("country")
)

display(df_hashed.limit(3))

# COMMAND ----------

# DBTITLE 1,Write Hashed Data to Target Table
# Define target table path
target_table = "pii_demo.pii_target.customers_hashed"

# Write the hashed dataframe to the target table
# Using overwrite mode to replace any existing data
df_hashed.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(target_table)

print(f"✓ Hashed data successfully written to: {target_table}")
print(f"✓ Total records written: {df_hashed.count()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pii_demo.pii_target.customers_hashed