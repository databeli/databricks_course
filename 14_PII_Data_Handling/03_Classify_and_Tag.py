# Databricks notebook source
df_source = spark.read.table("pii_demo.pii_source.customers")
df_source.write.mode("overwrite").saveAsTable("pii_demo.pii_target.customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pii_demo.pii_target.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- FULL NAME (Direct PII)
# MAGIC ALTER TABLE pii_demo.pii_target.customers
# MAGIC ALTER COLUMN full_name
# MAGIC SET TAGS (
# MAGIC   'pii' = 'true',
# MAGIC   'pii_type' = 'direct',
# MAGIC   'classification' = 'name'
# MAGIC );
# MAGIC
# MAGIC -- EMAIL (Direct PII)
# MAGIC ALTER TABLE pii_demo.pii_target.customers
# MAGIC ALTER COLUMN email
# MAGIC SET TAGS (
# MAGIC   'pii' = 'true',
# MAGIC   'pii_type' = 'direct',
# MAGIC   'classification' = 'email'
# MAGIC );
# MAGIC
# MAGIC -- PHONE (Direct PII)
# MAGIC ALTER TABLE pii_demo.pii_target.customers
# MAGIC ALTER COLUMN phone
# MAGIC SET TAGS (
# MAGIC   'pii' = 'true',
# MAGIC   'pii_type' = 'direct',
# MAGIC   'classification' = 'phone'
# MAGIC );
# MAGIC
# MAGIC -- SSN (Highly Sensitive Direct PII)
# MAGIC ALTER TABLE pii_demo.pii_target.customers
# MAGIC ALTER COLUMN ssn
# MAGIC SET TAGS (
# MAGIC   'pii' = 'true',
# MAGIC   'pii_type' = 'direct',
# MAGIC   'classification' = 'ssn'
# MAGIC );
# MAGIC
# MAGIC -- DATE OF BIRTH (Indirect PII)
# MAGIC ALTER TABLE pii_demo.pii_target.customers
# MAGIC ALTER COLUMN date_of_birth
# MAGIC SET TAGS (
# MAGIC   'pii' = 'true',
# MAGIC   'pii_type' = 'indirect',
# MAGIC   'classification' = 'dob'
# MAGIC );
# MAGIC
# MAGIC -- ADDRESS (Direct PII)
# MAGIC ALTER TABLE pii_demo.pii_target.customers
# MAGIC ALTER COLUMN address
# MAGIC SET TAGS (
# MAGIC   'pii' = 'true',
# MAGIC   'pii_type' = 'direct',
# MAGIC   'classification' = 'address'
# MAGIC );
# MAGIC
# MAGIC -- CITY (Indirect PII)
# MAGIC ALTER TABLE pii_demo.pii_target.customers
# MAGIC ALTER COLUMN city
# MAGIC SET TAGS (
# MAGIC   'pii' = 'true',
# MAGIC   'pii_type' = 'indirect',
# MAGIC   'classification' = 'location'
# MAGIC );
# MAGIC
# MAGIC -- CUSTOMER ID (Non-PII)
# MAGIC ALTER TABLE pii_demo.pii_target.customers
# MAGIC ALTER COLUMN customer_id
# MAGIC SET TAGS (
# MAGIC   'pii' = 'false'
# MAGIC );
# MAGIC
# MAGIC -- COUNTRY (Non-PII)
# MAGIC ALTER TABLE pii_demo.pii_target.customers
# MAGIC ALTER COLUMN country
# MAGIC SET TAGS (
# MAGIC   'pii' = 'false'
# MAGIC );