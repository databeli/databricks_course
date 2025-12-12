# Databricks notebook source
# MAGIC %md
# MAGIC # Secrets

# COMMAND ----------

# MAGIC %md
# MAGIC ## Secrets -> listScopes
# MAGIC
# MAGIC Lists the available scopes.
# MAGIC
# MAGIC listScopes: Seq

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Secrets -> list
# MAGIC
# MAGIC Lists the metadata for secrets within the specified scope.
# MAGIC
# MAGIC list(scope: String): Seq

# COMMAND ----------

dbutils.secrets.list("my_secret_scope_2")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Secrets -> Get
# MAGIC
# MAGIC Gets the string representation of a secret value for the specified secrets scope and key.
# MAGIC
# MAGIC get(scope: String, key: String): String

# COMMAND ----------

my_key=dbutils.secrets.get(scope="my_secret_scope_2", key="my_key")

# COMMAND ----------

print(my_key)

# COMMAND ----------

for ch in my_key:
  print(ch)