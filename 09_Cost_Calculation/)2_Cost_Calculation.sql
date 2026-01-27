-- Databricks notebook source
SELECT * FROM system.billing.usage ORDER BY usage_start_time DESC

-- COMMAND ----------

select * from system.billing.usage where usage_metadata.job_run_id='481719574637711'

-- COMMAND ----------

select usage_start_time,usage_end_time,usage_quantity,usage_unit,sku_name from system.billing.usage where usage_metadata.job_run_id='481719574637711'

-- COMMAND ----------

select * from system.billing.list_prices

-- COMMAND ----------

select * from system.billing.list_prices where sku_name='PREMIUM_JOBS_SERVERLESS_COMPUTE_US_EAST_OHIO'

-- COMMAND ----------

select pricing.effective_list.default as price from system.billing.list_prices where sku_name='PREMIUM_JOBS_SERVERLESS_COMPUTE_US_EAST_OHIO' and price_end_time is null

-- COMMAND ----------


select usage_quantity,price,usage_quantity*price as cost from (
select usage_start_time,usage_end_time,usage_quantity,usage_unit,sku_name from system.billing.usage where usage_metadata.job_run_id='481719574637711') usage

join

(select pricing.effective_list.default as price,sku_name from system.billing.list_prices where price_end_time is null) prices
on usage.sku_name=prices.sku_name

-- COMMAND ----------


select sum(cost) as cost from (
select usage_quantity,price,usage_quantity*price as cost from (
select usage_start_time,usage_end_time,usage_quantity,usage_unit,sku_name from system.billing.usage where usage_metadata.job_run_id='481719574637711') usage

join

(select pricing.effective_list.default as price,sku_name from system.billing.list_prices where price_end_time is null) prices
on usage.sku_name=prices.sku_name
)x