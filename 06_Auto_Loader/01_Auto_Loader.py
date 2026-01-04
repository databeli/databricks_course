# Databricks notebook source
# MAGIC %sql
# MAGIC drop table training.auto_loader_demo.demo_table5

# COMMAND ----------

dbutils.fs.rm("/Volumes/training/my_schema/my_volume/auto_loader/json/",recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Stream

# COMMAND ----------

df_stream=spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/Volumes/training/my_schema/my_volume/auto_loader/json/schema/") \
  .load("/Volumes/training/my_schema/my_volume/auto_loader/json/input/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Stream - Location

# COMMAND ----------

df_stream.writeStream \
  .option("checkpointLocation", "/Volumes/training/my_schema/my_volume/auto_loader/json/checkpoint") \
  .trigger(availableNow=True) \
  .start("/Volumes/training/my_schema/my_volume/auto_loader/json/output")\
    .awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Stream - Delta

# COMMAND ----------

df_stream.writeStream \
  .option("checkpointLocation", "/Volumes/training/my_schema/my_volume/auto_loader/json/checkpoint2") \
  .trigger(availableNow=True) \
  .table("training.auto_loader_demo.demo_table")\
    .awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from training.auto_loader_demo.demo_table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cloud_files_state('/Volumes/training/my_schema/my_volume/auto_loader/json/checkpoint4');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Option 1 - Provide Schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("customer_name", StringType(), nullable=True),
    StructField("product", StringType(), nullable=True),
    StructField("quantity", IntegerType(), nullable=True),
    StructField("price", IntegerType(), nullable=True),
    StructField("remarks", StringType(), nullable=True)
])

spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "json") \
  .schema(schema)\
  .option("cloudFiles.schemaLocation", "/Volumes/training/my_schema/my_volume/auto_loader/json/schema3/") \
  .load("/Volumes/training/my_schema/my_volume/auto_loader/json/input/")\
    .writeStream \
  .option("checkpointLocation", "/Volumes/training/my_schema/my_volume/auto_loader/json/checkpoint3") \
  .trigger(availableNow=True) \
  .table("training.auto_loader_demo.demo_table3")\
  .awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from training.auto_loader_demo.demo_table3 order by id desc

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Option 2 - Infer Column Data Types

# COMMAND ----------

spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "json") \
  .option("cloudFiles.inferColumnTypes", "true") \
  .option("cloudFiles.schemaLocation", "/Volumes/training/my_schema/my_volume/auto_loader/json/schema4/") \
  .load("/Volumes/training/my_schema/my_volume/auto_loader/json/input/")\
    .writeStream \
  .option("checkpointLocation", "/Volumes/training/my_schema/my_volume/auto_loader/json/checkpoint4") \
  .trigger(availableNow=True) \
  .table("training.auto_loader_demo.demo_table4")\
  .awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from training.auto_loader_demo.demo_table4 order by id desc

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handle Schema Changes - Rescue Data

# COMMAND ----------

spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "json") \
  .option("cloudFiles.inferColumnTypes", "true") \
  .option("cloudFiles.schemaEvolutionMode", "rescue") \
  .option("cloudFiles.schemaLocation", "/Volumes/training/my_schema/my_volume/auto_loader/json/schema5/") \
  .load("/Volumes/training/my_schema/my_volume/auto_loader/json/input/")\
    .writeStream \
  .option("checkpointLocation", "/Volumes/training/my_schema/my_volume/auto_loader/json/checkpoint5") \
  .trigger(availableNow=True) \
  .table("training.auto_loader_demo.demo_table5")\
  .awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from training.auto_loader_demo.demo_table5 order by id desc