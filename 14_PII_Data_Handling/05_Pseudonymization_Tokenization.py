# Databricks notebook source
# DBTITLE 1,Import libraries and read source data
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import uuid

# Read the source customer table
source_df = spark.table("pii_demo.pii_source.customers")

print(f"Total records: {source_df.count()}")
display(source_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Generate email tokens and create lookup table
# Create a UDF to generate unique tokens (UUIDs)
def generate_token():
    return str(uuid.uuid4())

generate_token_udf = F.udf(generate_token)

# Get distinct emails and generate tokens for each
email_lookup_df = source_df.select("email").distinct() \
    .withColumn("token", generate_token_udf())

print(f"Unique emails: {email_lookup_df.count()}")
display(email_lookup_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Join source data with tokens and create tokenized table
# Join the source data with the email lookup to replace emails with tokens
customers_tokenized_df = source_df.join(email_lookup_df, on="email", how="left") \
    .select(
        "customer_id",
        "full_name",
        F.col("token").alias("email_token"),
        "phone",
        "ssn",
        "date_of_birth",
        "address",
        "city",
        "country"
    )

print(f"Tokenized records: {customers_tokenized_df.count()}")
display(customers_tokenized_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Write tokenized data and lookup table to target locations
# Write the tokenized customer data to target table
customers_tokenized_df.write.mode("overwrite").saveAsTable("pii_demo.pii_target.customers_tokenized")

# Write the email token lookup table
email_lookup_df.select("token", "email").write.mode("overwrite").saveAsTable("pii_demo.pii_target.email_token_lookup")

print("✓ Tokenization complete!")
print("✓ Created table: pii_demo.pii_target.customers_tokenized")
print("✓ Created table: pii_demo.pii_target.email_token_lookup")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pii_demo.pii_target.customers_tokenized

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pii_demo.pii_target.email_token_lookup