-- Databricks notebook source
-- DBTITLE 1,Created View
create or replace view pii_demo.pii_target.customers_suppressed
as select 
customer_id,
country
 from pii_demo.pii_target.customers

-- COMMAND ----------

select* from pii_demo.pii_target.customers_suppressed