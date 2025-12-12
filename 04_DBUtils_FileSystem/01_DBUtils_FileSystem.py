# Databricks notebook source
# MAGIC %md
# MAGIC #DBUtils -> fs

# COMMAND ----------

# MAGIC %md
# MAGIC ## DBUtils -> fs -> ls
# MAGIC
# MAGIC Lists the contents of a directory.
# MAGIC
# MAGIC ls(dir: String): Seq

# COMMAND ----------

dbutils.fs.ls("/Volumes/dbutils_catalog/default/dbutils_volume/input")

# COMMAND ----------

df=dbutils.fs.ls("/Volumes/dbutils_catalog/default/dbutils_volume/input")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DBUtils -> fs -> cp
# MAGIC
# MAGIC Copies a file or directory, possibly across filesystems.
# MAGIC
# MAGIC cp(from: String, to: String, recurse: boolean = false): boolean

# COMMAND ----------

dbutils.fs.cp("/Volumes/dbutils_catalog/default/dbutils_volume/input/sales.csv","/Volumes/dbutils_catalog/default/dbutils_volume/output/sales.csv")

# COMMAND ----------

dbutils.fs.cp("/Volumes/dbutils_catalog/default/dbutils_volume/input","/Volumes/dbutils_catalog/default/dbutils_volume/output",recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DBUtils -> fs -> mv
# MAGIC
# MAGIC Moves a file or directory, possibly across FileSystems
# MAGIC
# MAGIC mv(from: String, to: String, recurse: boolean = false): boolean

# COMMAND ----------

dbutils.fs.mv("/Volumes/dbutils_catalog/default/dbutils_volume/input/sales.csv","/Volumes/dbutils_catalog/default/dbutils_volume/output/sales.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## DBUtils -> fs -> rm
# MAGIC
# MAGIC Removes a file or directory
# MAGIC
# MAGIC rm(dir: String, recurse: boolean = false): boolean

# COMMAND ----------

dbutils.fs.rm("/Volumes/dbutils_catalog/default/dbutils_volume/output/sales.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## DBUtils -> fs -> mkdirs
# MAGIC
# MAGIC Creates the given directory if it does not exist. Also creates any necessary parent directories.
# MAGIC
# MAGIC mkdirs(dir: String): boolean

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/dbutils_catalog/default/dbutils_volume/output3/output4")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dbutils -> fs -> head
# MAGIC
# MAGIC Returns up to the specified maximum number of bytes in the given file. The bytes are returned as a UTF-8 encoded string
# MAGIC
# MAGIC head(file: String, max_bytes: int = 65536): String

# COMMAND ----------

# dbutils.fs.head("/Volumes/dbutils_catalog/default/dbutils_volume/input/sales_corrupted.csv")
dbutils.fs.head("/Volumes/dbutils_catalog/default/dbutils_volume/input/sales_corrupted.csv",25)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DBUtils -> fs -> put
# MAGIC
# MAGIC Writes the specified string to a file. The string is UTF-8 encoded.
# MAGIC
# MAGIC put(file: String, contents: String, overwrite: boolean = false): boolean

# COMMAND ----------

dbutils.fs.put("/Volumes/dbutils_catalog/default/dbutils_volume/output/pu_test.txt", "Hello, Narender!", True)