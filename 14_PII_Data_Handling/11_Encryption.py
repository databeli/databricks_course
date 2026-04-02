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
# Apply AES encryption to PII columns
# Keep non-PII columns (customer_id, country) unchanged
from pyspark.sql import functions as F

# Define encryption key (16, 24, or 32 bytes for AES-128, AES-192, or AES-256)
# In production, store this securely in Databricks Secrets
encryption_key = "MySecureKey12345"  # 16 bytes = AES-128

df_encrypted = df_source.select(
    # Non-PII column - keep as is
    F.col("customer_id"),
    
    # PII columns - apply AES encryption and encode to base64 for readability
    # 1. full_name: AES encryption
    F.base64(F.aes_encrypt(F.col("full_name"), F.lit(encryption_key))).alias("full_name"),
    
    # 2. email: AES encryption
    F.base64(F.aes_encrypt(F.col("email"), F.lit(encryption_key))).alias("email"),
    
    # 3. phone: AES encryption (cast bigint to string first)
    F.base64(F.aes_encrypt(F.col("phone").cast("string"), F.lit(encryption_key))).alias("phone"),
    
    # 4. ssn: AES encryption
    F.base64(F.aes_encrypt(F.col("ssn"), F.lit(encryption_key))).alias("ssn"),
    
    # 5. date_of_birth: AES encryption (cast date to string first)
    F.base64(F.aes_encrypt(F.col("date_of_birth").cast("string"), F.lit(encryption_key))).alias("date_of_birth"),
    
    # 6. address: AES encryption
    F.base64(F.aes_encrypt(F.col("address"), F.lit(encryption_key))).alias("address"),
    
    # 7. city: AES encryption
    F.base64(F.aes_encrypt(F.col("city"), F.lit(encryption_key))).alias("city"),
    
    # Non-PII column - keep as is
    F.col("country")
)

print("✓ AES encryption applied to all PII columns")
display(df_encrypted.limit(3))

# COMMAND ----------

# DBTITLE 1,Write Hashed Data to Target Table
# Define target table path
target_table = "pii_demo.pii_target.customers_encrypted"

# Write the encrypted dataframe to the target table
# Using overwrite mode to replace any existing data
df_encrypted.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(target_table)

print(f"✓ Encrypted data successfully written to: {target_table}")
print(f"✓ Total records written: {df_encrypted.count()}")

# COMMAND ----------

# DBTITLE 1,Query Encrypted Data
# MAGIC %sql
# MAGIC SELECT * FROM pii_demo.pii_target.customers_encrypted

# COMMAND ----------

# DBTITLE 1,Decrypt Data to Show Actual Values
# Decrypt the encrypted data to show actual values
# Use the same encryption_key variable for decryption

query = f"""
SELECT 
    customer_id,
    CAST(aes_decrypt(unbase64(full_name), '{encryption_key}') AS STRING) AS full_name,
    CAST(aes_decrypt(unbase64(email), '{encryption_key}') AS STRING) AS email,
    CAST(aes_decrypt(unbase64(phone), '{encryption_key}') AS STRING) AS phone,
    CAST(aes_decrypt(unbase64(ssn), '{encryption_key}') AS STRING) AS ssn,
    CAST(aes_decrypt(unbase64(date_of_birth), '{encryption_key}') AS STRING) AS date_of_birth,
    CAST(aes_decrypt(unbase64(address), '{encryption_key}') AS STRING) AS address,
    CAST(aes_decrypt(unbase64(city), '{encryption_key}') AS STRING) AS city,
    country
FROM pii_demo.pii_target.customers_encrypted
LIMIT 10
"""

df_decrypted = spark.sql(query)
print("✓ Data decrypted successfully")
display(df_decrypted)

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists pii_demo.access_control

# COMMAND ----------

# DBTITLE 1,Create UDF for Decryption
# MAGIC %sql
# MAGIC -- Create a UDF in the access_control schema to decrypt PII data
# MAGIC -- This UDF can be used by authorized users to decrypt encrypted columns
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION pii_demo.access_control.decrypt_pii(encrypted_value STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CAST(aes_decrypt(unbase64(encrypted_value), 'MySecureKey12345') AS STRING);

# COMMAND ----------

# DBTITLE 1,Query Encrypted Data Using UDF
# MAGIC %sql
# MAGIC -- Use the UDF to decrypt and access encrypted columns
# MAGIC -- This approach allows for centralized access control and key management
# MAGIC
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     pii_demo.access_control.decrypt_pii(full_name) AS full_name,
# MAGIC     pii_demo.access_control.decrypt_pii(email) AS email,
# MAGIC     pii_demo.access_control.decrypt_pii(phone) AS phone,
# MAGIC     pii_demo.access_control.decrypt_pii(ssn) AS ssn,
# MAGIC     pii_demo.access_control.decrypt_pii(date_of_birth) AS date_of_birth,
# MAGIC     pii_demo.access_control.decrypt_pii(address) AS address,
# MAGIC     pii_demo.access_control.decrypt_pii(city) AS city,
# MAGIC     country
# MAGIC FROM pii_demo.pii_target.customers_encrypted
# MAGIC LIMIT 5;