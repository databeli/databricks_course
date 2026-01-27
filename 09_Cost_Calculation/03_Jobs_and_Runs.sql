-- Databricks notebook source
select * from system.lakeflow.jobs where name = 'Demo 3'

-- COMMAND ----------

select * from system.lakeflow.job_tasks where job_id='427853118048741'

-- COMMAND ----------

select * from system.lakeflow.job_run_timeline where job_id='427853118048741'

-- COMMAND ----------

select * from system.lakeflow.job_task_run_timeline where job_id='427853118048741'