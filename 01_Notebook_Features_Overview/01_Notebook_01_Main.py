# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Notebook – Full Feature Walkthrough
# MAGIC
# MAGIC This notebook explains **all key features** of Databricks notebooks, including:
# MAGIC
# MAGIC - Basic notebook UI (cells, languages, shortcuts)
# MAGIC - Magic commands (`%sql`, `%md`, `%fs`, `%sh`, `%pip`, `%run`, etc.)
# MAGIC - Working with data (tables, DataFrames)
# MAGIC - Widgets (interactive parameters)
# MAGIC - Visualizations
# MAGIC - Filesystem access
# MAGIC - dbutils (secrets, widgets, filesystem, etc.)
# MAGIC - Versioning & revision history (concept)
# MAGIC - Collaboration features (comments, tags, etc.) – concept
# MAGIC
# MAGIC > Run the cells one by one and read the explanations in the markdown cells.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. What is a Databricks Notebook?
# MAGIC
# MAGIC A Databricks notebook is an interactive environment for:
# MAGIC
# MAGIC - Writing and running **code** (Python, SQL, Scala, R)
# MAGIC - Combining **code + markdown** documentation
# MAGIC - **Experimenting** with data
# MAGIC - **Visualizing** results
# MAGIC - Collaborating with others
# MAGIC
# MAGIC Each notebook is made up of **cells**:
# MAGIC - **Code cells** – run code (Python / SQL / etc.)
# MAGIC - **Markdown cells** – formatted text, images, headings
# MAGIC
# MAGIC You can:
# MAGIC - Add a cell – using the `+` button or keyboard shortcuts
# MAGIC - Move cells – drag the left side handle
# MAGIC - Run a single cell – `Shift + Enter`
# MAGIC - Run all cells – from the `Run all` or `Run selected` menu
# MAGIC

# COMMAND ----------

# DBTITLE 1,Python Basic Example
# 2. Basic Python execution in a Databricks notebook

x = 10
y = 5
sum_xy = x + y

print(f"x = {x}, y = {y}, x + y = {sum_xy}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Using Multiple Languages with Magic Commands
# MAGIC
# MAGIC A Databricks notebook has a **default language** (e.g., Python),  
# MAGIC but you can run other languages in any cell using **magic commands**:
# MAGIC
# MAGIC - `%python` – Python
# MAGIC - `%sql` – SQL
# MAGIC - `%scala` – Scala
# MAGIC - `%r` – R
# MAGIC - `%md` – Markdown inside a code cell
# MAGIC - `%sh` – Shell commands
# MAGIC - `%fs` – Filesystem commands
# MAGIC - `%pip` – Install Python packages
# MAGIC
# MAGIC You put the magic command at the **top of the cell**, as the first line.
# MAGIC

# COMMAND ----------

# DBTITLE 1,SQL Magic Command
# MAGIC %sql
# MAGIC SELECT 'Hello from SQL in a Python notebook!' AS message
# MAGIC
# MAGIC -- # 3. Running SQL in a Python notebook using %sql
# MAGIC
# MAGIC -- # This cell demonstrates SQL. In Databricks, change the cell language to "SQL"
# MAGIC -- # OR keep it Python default and use %sql on first line.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC this is a samole cell

# COMMAND ----------

# MAGIC %md
# MAGIC ## # this is my cell

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Using %md to write Markdown inside a cell
# MAGIC
# MAGIC %md
# MAGIC ### This is rendered as a Markdown heading
# MAGIC
# MAGIC - You can use **bold**, *italic*, `code`
# MAGIC - You can write lists, tables, and more
# MAGIC - Very useful for documenting your analysis
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Accessing Filesystem with `%fs`
# MAGIC
# MAGIC Databricks provides a special filesystem abstraction called **DBFS (Databricks File System)**.
# MAGIC
# MAGIC Common `%fs` commands:
# MAGIC
# MAGIC - `%fs ls` – list files/directories  
# MAGIC - `%fs mkdirs /mnt/mydir` – create directories  
# MAGIC - `%fs rm -r /mnt/mydir` – remove directory  
# MAGIC - `%fs head /path/file` – preview file
# MAGIC
# MAGIC You can also use `dbutils.fs` in Python.
# MAGIC

# COMMAND ----------

# DBTITLE 1,FS Magic Command
# MAGIC %fs ls /Volumes/my_catalog_2/my_schema/my_volume/my_folder/

# COMMAND ----------

# DBTITLE 1,FS using DB Utils
# 6. Using dbutils.fs in Python

# List files in root
dbutils.fs.ls("/Volumes/my_catalog_2/my_schema/my_volume/my_folder/")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Installing Libraries with `%pip`
# MAGIC
# MAGIC In Databricks notebooks, you can install Python packages at runtime using `%pip`:
# MAGIC
# MAGIC - `%pip install some-package`
# MAGIC - Works per cluster & environment
# MAGIC - The notebook will typically restart the Python process after installing
# MAGIC
# MAGIC > Note: In production, prefer **cluster-level** or **workspace** libraries when possible.
# MAGIC

# COMMAND ----------

# DBTITLE 1,PIP Magic Command
# 7. Example of installing a package using %pip
# (This is just a demonstration. You may or may not need this package.)

%pip install textblob

from textblob import TextBlob

TextBlob("Databricks notebooks are awesome!").sentiment

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Running Shell Commands with `%sh`
# MAGIC
# MAGIC You can use `%sh` to run **shell commands** on the driver node:
# MAGIC
# MAGIC Examples:
# MAGIC - `%sh ls`
# MAGIC - `%sh echo "Hello from shell"`
# MAGIC - `%sh python --version`
# MAGIC
# MAGIC Useful for quick environment checks and debugging.
# MAGIC

# COMMAND ----------

# DBTITLE 1,SH Magic Command
# MAGIC %sh
# MAGIC echo "Current working directory:"
# MAGIC pwd
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Running Scala Code
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC val name='narender'
# MAGIC println(name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Skip Magic Command

# COMMAND ----------

# MAGIC %skip
# MAGIC print("Skip this cell during main execution of the code")
# MAGIC sdfsdfds
# MAGIC
# MAGIC
# MAGIC sdfdsfsdfsfsfsdf

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Working with Data: Tables & DataFrames
# MAGIC
# MAGIC In Databricks, you typically work with:
# MAGIC
# MAGIC - **Tables** in a catalog (e.g., `hive_metastore`, `unity catalog`)
# MAGIC - **Spark DataFrames**
# MAGIC
# MAGIC Common ways to create data:
# MAGIC - `spark.range(...)`
# MAGIC - `spark.read` from file formats (CSV, JSON, Parquet, Delta)
# MAGIC - `CREATE TABLE` / `INSERT` via SQL
# MAGIC

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Working with Data
# 9. Creating a simple Spark DataFrame

data = [
    (1, "Alice", 1000.0),
    (2, "Bob", 1500.5),
    (3, "Charlie", 800.25)
]

columns = ["id", "name", "salary"]

df = spark.createDataFrame(data, columns)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Visualizations
# MAGIC
# MAGIC Databricks has built-in visualizations:
# MAGIC
# MAGIC - When you use `display(df)` or run a SQL query:
# MAGIC   - You can switch between **Table**, **Bar**, **Line**, **Map**, etc.
# MAGIC   - Use the **"Plot Options"** button above the result
# MAGIC
# MAGIC You can:
# MAGIC - Change **group by**, **aggregation**, **x/y-axis**
# MAGIC - Save the plot as a **Dashboard** (depending on workspace features)
# MAGIC

# COMMAND ----------

# DBTITLE 1,Visualization
# 11. Visualization demo

from pyspark.sql.functions import col

# Create sample data with categories
sales_data = [
    ("2025-01-01", "ProductA", 100),
    ("2025-01-02", "ProductA", 120),
    ("2025-01-01", "ProductB", 80),
    ("2025-01-02", "ProductB", 90),
]

sales_columns = ["date", "product", "revenue"]
sales_df = spark.createDataFrame(sales_data, sales_columns)

display(sales_df)

# After running, click on "Plot Options" to create a chart.


# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Widgets (Interactive Notebook Parameters)
# MAGIC
# MAGIC Widgets allow you to **parameterize** your notebook.
# MAGIC
# MAGIC Types of widgets:
# MAGIC - `text` – free text
# MAGIC - `dropdown` – one value from a list
# MAGIC - `combobox` – Combination of text and dropdown
# MAGIC - `multiselect` – multiple values
# MAGIC
# MAGIC Core APIs:
# MAGIC - `dbutils.widgets.text(name, defaultValue, label)`
# MAGIC - `dbutils.widgets.dropdown(name, defaultValue, choices, label)`
# MAGIC - `dbutils.widgets.get(name)` – to read the value
# MAGIC - `dbutils.widgets.remove(name)` / `removeAll()`
# MAGIC
# MAGIC Widgets are great for:
# MAGIC - Running the same notebook with different inputs
# MAGIC - Using with **Jobs** for scheduled runs
# MAGIC

# COMMAND ----------

# DBTITLE 1,Widgets
# 12. Creating widgets

dbutils.widgets.removeAll()

dbutils.widgets.text("p_name", "Alice", "Employee name")
dbutils.widgets.dropdown("p_min_salary", "0", ["0", "500", "1000"], "Min Salary")

print("Widgets created. Change the values in the top widget panel and re-run the next cell.")

# COMMAND ----------

# 13. Reading widget values and applying filters

name = dbutils.widgets.get("p_name")
salary = float(dbutils.widgets.get("p_min_salary"))

print(f"name: {name}, salary: {salary}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Reusing Code with `%run`
# MAGIC
# MAGIC You can use `%run` to **include another notebook** into the current one.
# MAGIC
# MAGIC - Syntax: `%run /Path/To/OtherNotebook`
# MAGIC - All functions, variables, and classes from that notebook become available
# MAGIC
# MAGIC Useful for:
# MAGIC - Creating **utility** notebooks (e.g., common functions)
# MAGIC - Keeping code modular and organized
# MAGIC

# COMMAND ----------

# DBTITLE 1,RUN Magic Command
# MAGIC %run "/Workspace/Users/databeli14@gmail.com/PySpark and Databricks Complete Course/03 Databricks/01_Notebook_Features_Overview/01_Notebook_02_Helper"
# MAGIC

# COMMAND ----------

lower_string="my name is narender"
upper_string=uppper_function(lower_string)
print(upper_string)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Revision History & Checkpoints (Concept)
# MAGIC
# MAGIC Databricks notebooks support:
# MAGIC
# MAGIC - **Revision history**:
# MAGIC   - View, compare, and restore previous versions
# MAGIC
# MAGIC - **Autosave**:
# MAGIC   - Notebooks are saved automatically as you edit
# MAGIC
# MAGIC - **Export & Import**:
# MAGIC   - Export as `.dbc`, `.html`, or `.ipynb` (depending on workspace)
# MAGIC   - Import notebooks from files or from Git
# MAGIC
# MAGIC You can use this to roll back experiments, review changes, and share work.
# MAGIC