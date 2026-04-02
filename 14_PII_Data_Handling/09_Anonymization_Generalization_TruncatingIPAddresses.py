# Databricks notebook source
# DBTITLE 1,Load source data from web_logs
# Read the source data
df_web_logs = spark.table("pii_demo.pii_source.web_logs")

# Display sample data to verify
display(df_web_logs.limit(5))

# COMMAND ----------

# DBTITLE 1,Truncate IP addresses by replacing last byte with 0
from pyspark.sql.functions import col, regexp_replace

# Truncate IP address by replacing the last octet with 0
# Pattern matches the last octet (last number after the final dot)
df_truncated = df_web_logs.withColumn(
    "ip_address",
    regexp_replace(col("ip_address"), r"\d+$", "0")
)

# Display sample of truncated data
display(df_truncated.limit(5))

# COMMAND ----------

# DBTITLE 1,Write truncated data to target table
# Write the truncated data to the target table
df_truncated.write.mode("overwrite").saveAsTable("pii_demo.pii_target.web_logs_ip_truncated")

print("Data successfully written to pii_demo.pii_target.web_logs_ip_truncated")
print(f"Total records processed: {df_truncated.count()}")

# COMMAND ----------

# DBTITLE 1,Verify the truncated IP addresses in target table
# MAGIC %sql
# MAGIC select * from pii_demo.pii_target.web_logs_ip_truncated