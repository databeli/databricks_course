-- Databricks notebook source
-- DBTITLE 1,Show sample customer data
-- Sample data from customers table
SELECT * FROM pii_demo.pii_target.customers LIMIT 5

-- COMMAND ----------

-- DBTITLE 1,Create mask_full_name function
-- Masking function for full_name
-- Admin group sees original, others see first letter + ***
CREATE OR REPLACE FUNCTION pii_demo.access_control.mask_full_name(name STRING)
RETURNS STRING
RETURN CASE 
  WHEN is_account_group_member('admin') THEN name
  ELSE CONCAT(LEFT(name, 1), '***')
END

-- COMMAND ----------

-- DBTITLE 1,Create mask_email function
-- Masking function for email
-- Admin group sees original, others see first char + *** + domain
CREATE OR REPLACE FUNCTION pii_demo.access_control.mask_email(email STRING)
RETURNS STRING
RETURN CASE 
  WHEN is_account_group_member('admin') THEN email
  ELSE CONCAT(LEFT(email, 1), '***@', SPLIT(email, '@')[1])
END

-- COMMAND ----------

-- DBTITLE 1,Create mask_phone function
-- Masking function for phone
-- Admin group sees original, others see ***-***-XXXX (last 4 digits)
CREATE OR REPLACE FUNCTION pii_demo.access_control.mask_phone(phone BIGINT)
RETURNS STRING
RETURN CASE 
  WHEN is_account_group_member('admin') THEN CAST(phone AS STRING)
  ELSE CONCAT('***-***-', RIGHT(CAST(phone AS STRING), 4))
END

-- COMMAND ----------

-- DBTITLE 1,Create mask_ssn function
-- Masking function for SSN
-- Admin group sees original, others see XXX-XX-#### (last 4 digits only)
CREATE OR REPLACE FUNCTION pii_demo.access_control.mask_ssn(ssn STRING)
RETURNS STRING
RETURN CASE 
  WHEN is_account_group_member('admin') THEN ssn
  ELSE CONCAT('XXX-XX-', RIGHT(ssn, 4))
END

-- COMMAND ----------

-- DBTITLE 1,Create mask_dob function
-- Masking function for date_of_birth
-- Admin group sees original, others see only year
CREATE OR REPLACE FUNCTION pii_demo.access_control.mask_dob(dob DATE)
RETURNS STRING
RETURN CASE 
  WHEN is_account_group_member('admin') THEN CAST(dob AS STRING)
  ELSE CONCAT(YEAR(dob), '-XX-XX')
END

-- COMMAND ----------

-- DBTITLE 1,Create mask_address function
-- Masking function for address
-- Admin group sees original, others see masked address
CREATE OR REPLACE FUNCTION pii_demo.access_control.mask_address(address STRING)
RETURNS STRING
RETURN CASE 
  WHEN is_account_group_member('admin') THEN address
  ELSE '*** *** ***'
END

-- COMMAND ----------

-- DBTITLE 1,Create mask_city function
-- Masking function for city
-- Admin group sees original, others see first 2 chars + ***
CREATE OR REPLACE FUNCTION pii_demo.access_control.mask_city(city STRING)
RETURNS STRING
RETURN CASE 
  WHEN is_account_group_member('admin') THEN city
  ELSE CONCAT(LEFT(city, 2), '***')
END

-- COMMAND ----------

-- DBTITLE 1,Create masked view with all functions
-- Apply masking function to full_name column
ALTER TABLE pii_demo.pii_target.customers 
ALTER COLUMN full_name SET MASK pii_demo.access_control.mask_full_name

-- COMMAND ----------

-- DBTITLE 1,Test masked view
-- Apply masking function to email column
ALTER TABLE pii_demo.pii_target.customers 
ALTER COLUMN email SET MASK pii_demo.access_control.mask_email

-- COMMAND ----------

-- DBTITLE 1,Apply mask to phone column
-- Apply masking function to phone column
ALTER TABLE pii_demo.pii_target.customers 
ALTER COLUMN phone SET MASK pii_demo.access_control.mask_phone

-- COMMAND ----------

-- DBTITLE 1,Apply mask to ssn column
-- Apply masking function to ssn column
ALTER TABLE pii_demo.pii_target.customers 
ALTER COLUMN ssn SET MASK pii_demo.access_control.mask_ssn

-- COMMAND ----------

-- DBTITLE 1,Apply mask to date_of_birth column
-- Apply masking function to date_of_birth column
ALTER TABLE pii_demo.pii_target.customers 
ALTER COLUMN date_of_birth SET MASK pii_demo.access_control.mask_dob

-- COMMAND ----------

-- DBTITLE 1,Apply mask to address column
-- Apply masking function to address column
ALTER TABLE pii_demo.pii_target.customers 
ALTER COLUMN address SET MASK pii_demo.access_control.mask_address

-- COMMAND ----------

-- DBTITLE 1,Apply mask to city column
-- Apply masking function to city column
ALTER TABLE pii_demo.pii_target.customers 
ALTER COLUMN city SET MASK pii_demo.access_control.mask_city

-- COMMAND ----------

-- DBTITLE 1,Test masked table
-- Test the masked table - query directly from the original table
-- If you're in admin group, you'll see original values
-- Otherwise, you'll see masked values based on the column masks
SELECT * FROM pii_demo.pii_target.customers LIMIT 5