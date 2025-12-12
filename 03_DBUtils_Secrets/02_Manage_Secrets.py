# Databricks notebook source
# MAGIC %md
# MAGIC ## Get API URL and Token

# COMMAND ----------

# Python cell in the same workspace notebook
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

api_url  = ctx.apiUrl().getOrElse(None)     # e.g. https://adb-...azuredatabricks.net
api_token = ctx.apiToken().getOrElse(None)  # personal access token for this session

print(api_url)
print(api_token)  # handle securely, do not log in real code


# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Secret Scope

# COMMAND ----------

import requests
import json

# ----------------------------------------
# Configuration
# ----------------------------------------
DATABRICKS_INSTANCE = api_url  # Replace with your workspace URL
DATABRICKS_TOKEN = api_token  # Replace with your PAT

scope_name = "my_secret_scope_2"  # Scope to be created
backend_type = "DATABRICKS"     # Use "AZURE_KEYVAULT" if integrating with Key Vault

# ----------------------------------------
# API Endpoint
# ----------------------------------------
url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/scopes/create"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "scope": scope_name
}

# ----------------------------------------
# Send request
# ----------------------------------------
response = requests.post(url, headers=headers, data=json.dumps(payload))

# ----------------------------------------
# Handle response
# ----------------------------------------
if response.status_code == 200:
    print(f"Secret scope '{scope_name}' created successfully.")
else:
    print("Failed to create secret scope.")
    print("Status Code:", response.status_code)
    print("Response:", response.text)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Secret in Secret Scope

# COMMAND ----------

import requests
import json

scope = "my_secret_scope_2"           # Already existing scope
secret_key = "my_key"        # Name of the secret entry
secret_value = "<Your token or password>" # Value to store securely

# -------------------------------------------------
# API Endpoint
# -------------------------------------------------
url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/put"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "scope": scope,
    "key": secret_key,
    "string_value": secret_value
}

# -------------------------------------------------
# Send Request
# -------------------------------------------------
response = requests.post(url, headers=headers, data=json.dumps(payload))

# -------------------------------------------------
# Output
# -------------------------------------------------
if response.status_code == 200:
    print(f"Secret '{secret_key}' created successfully in scope '{scope}'.")
else:
    print("Failed to create secret.")
    print("Status:", response.status_code)
    print("Response:", response.text)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Delete Scope and All Keys

# COMMAND ----------

import requests

# ------------------------------------------
# Configuration
# ------------------------------------------

scope = "my_secret_scope_2"  # Scope you want to delete

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}"
}

# ------------------------------------------
# 1. List secrets in the scope
# ------------------------------------------
list_url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/list?scope={scope}"
resp = requests.get(list_url, headers=headers)

if resp.status_code == 400:
    print(f"Scope '{scope}' does not exist.")
    exit()

secrets = resp.json().get("secrets", [])

# ------------------------------------------
# 2. Delete each secret in the scope
# ------------------------------------------
for s in secrets:
    key = s["key"]
    del_url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/delete"
    requests.post(del_url, headers=headers, json={"scope": scope, "key": key})
    print(f"Deleted secret: {key}")

# ------------------------------------------
# 3. Delete the scope
# ------------------------------------------
del_scope_url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/scopes/delete"
resp = requests.post(del_scope_url, headers=headers, json={"scope": scope})

if resp.status_code == 200:
    print(f"Scope '{scope}' deleted successfully.")
else:
    print("Failed to delete scope:", resp.text)
