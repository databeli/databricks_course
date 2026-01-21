# Databricks notebook source
# MAGIC %md
# MAGIC # 1: Generating Code with Databricks AI Assistant
# MAGIC
# MAGIC In this scenario, we will demonstrate how to use the Databricks AI Assistant to generate code for reading a CSV file into a Spark DataFrame and displaying its contents. The AI Assistant can help streamline the coding process by providing quick and efficient code snippets based on user requests.

# COMMAND ----------

# Generate code to read a CSV file into a DataFrame
df = spark.read.csv("path/to/your/file.csv", header=True, inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC # 2: Debugging Code with Databricks AI Assistant
# MAGIC
# MAGIC In this scenario, we will demonstrate how to use the Databricks AI Assistant to debug a piece of code that contains an intentional error. The AI Assistant can help identify issues and suggest corrections, making the debugging process more efficient.
# MAGIC
# MAGIC ## Example Code with Intentional Error
# MAGIC

# COMMAND ----------

data = [
    ("James", "Smith", "M", 30),
    ("Anna", "Rose", "F", 41)
]
columns = [
    "firstname",
    "lastname",
    "gender",
    "age"
]
df = spark.createDataFrame(
    data,
    columns
)

df_filtered = df.filter(
    df.gender == "M"
)
display(df_filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC # 3: Modifying the code with Natural Language using Databricks AI Assistant
# MAGIC
# MAGIC In this scenario, we will demonstrate how to filter a DataFrame using natural language prompts with the help of the Databricks AI Assistant. This feature allows users to interact with their data in a more intuitive way, enabling them to perform complex queries without needing to write traditional SQL or DataFrame code. 
# MAGIC
# MAGIC For example, if you have a DataFrame containing sales data, you can ask the AI Assistant to "show me all sales greater than $1000" and it will generate the appropriate code to filter the DataFrame accordingly.
# MAGIC
# MAGIC Here is an example code snippet that illustrates how to filter a DataFrame based on a natural language prompt:
# MAGIC

# COMMAND ----------

data = [
    ("James", "Smith", "M", 30, 4000),
    ("Anna", "Rose", "F", 41, 5000),
    ("Robert", "Williams", "M", 62, 7000),
    ("Maria", "Jones", "F", 29, 6000),
    ("Jen", "Brown", "F", 45, 8000)
]
columns = ["firstname", "lastname", "gender", "age", "salary"]
df = spark.createDataFrame(data, columns)

# Filter the DataFrame to show only females with a salary greater than 6000
df_female_high_salary = df.filter((df.gender == "F") & (df.salary > 6000))
display(df_female_high_salary)

# Group by gender and calculate the average salary
df_avg_salary = df.groupBy("gender").avg("salary")
display(df_avg_salary)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4: Finding Relevant Documentation with Databricks AI Assistant
# MAGIC
# MAGIC In this scenario, users can leverage the Databricks AI Assistant to quickly find relevant documentation for various Spark functions. This feature is particularly useful for developers and data scientists who need to reference specific functionalities or methods without having to navigate through extensive documentation manually.
# MAGIC
# MAGIC ### Example
# MAGIC To ask for documentation on a specific Spark function, you can phrase your request in a straightforward manner. For instance, if you want to learn about converting string to date, you could ask:
# MAGIC

# COMMAND ----------

# Provide me documenation for converting string to date

# COMMAND ----------

# MAGIC %md
# MAGIC # 5: Pretitfy the code using /prettify command
# MAGIC Prettifying code refers to the process of formatting code to make it more readable and organized. This often involves adjusting indentation, spacing, and line breaks, as well as ensuring consistent use of syntax. Prettified code is easier to understand, maintain, and debug, which is especially important in collaborative environments.
# MAGIC
# MAGIC In Databricks AI Assistant, you can use the `/prettify` command to automatically format your code. This command analyzes the provided code and applies best practices for readability.
# MAGIC
# MAGIC The next cell contains messy code for demonstration purposes. You can use the `/prettify` command to clean it up and enhance its readability.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     s.name AS name, 
# MAGIC     e.age AS age, 
# MAGIC     s.amount AS amount, 
# MAGIC     o.status AS status 
# MAGIC FROM 
# MAGIC     sales s 
# MAGIC JOIN 
# MAGIC     employees e ON s.employee_id = e.id 
# MAGIC LEFT JOIN 
# MAGIC     orders o ON o.customer_id = s.customer_id 
# MAGIC WHERE 
# MAGIC     s.amount > 1000 
# MAGIC     AND (e.age < 30 OR o.status = 'completed') 
# MAGIC ORDER BY 
# MAGIC     s.date DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC # 6: Put comments using /doc command
# MAGIC
# MAGIC The `/doc` command in Databricks notebooks is a powerful feature that allows users to add comments or documentation directly within their code cells. This functionality enhances the readability and maintainability of the code by providing context and explanations for complex logic or specific implementations.
# MAGIC
# MAGIC ## How to Use /doc
# MAGIC
# MAGIC To use the `/doc` command, simply type `/doc` at the beginning of a new line in a code cell. This will convert the line into a markdown cell, allowing you to write formatted text, including headings, lists, and links. This is particularly useful for documenting the purpose of the code, outlining the steps taken, or providing any necessary background information.
# MAGIC
# MAGIC ### Example of Using /doc
# MAGIC
# MAGIC Here’s an example of how to use the `/doc` command in a code cell:
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, rank
from pyspark.sql import Window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Complex PySpark Example") \
    .getOrCreate()

# Read CSV files into DataFrames
df1 = spark.read.csv("data/file1.csv", header=True, inferSchema=True)
df2 = spark.read.csv("data/file2.csv", header=True, inferSchema=True)

# Perform an inner join on the two DataFrames based on the 'id' column
joined_df = df1.join(df2, df1.id == df2.id, "inner")

# Group by 'category' and aggregate to count 'id' and calculate average 'value'
grouped_df = joined_df.groupBy("category").agg(
    count("id").alias("count"),
    avg("value").alias("average_value")
)

# Define a window specification to order by 'count'
window_spec = Window.orderBy("count")

# Add a rank column based on the count using the defined window specification
final_df = grouped_df.withColumn("rank", rank().over(window_spec))

# Display the final DataFrame
display(final_df)

# Stop the Spark session
spark.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC # 7: Optimize the code using /optimize Command 
# MAGIC
# MAGIC The `/optimize` command in Databricks notebooks is a powerful tool that helps users enhance the performance of their code written in Python, PySpark, and SQL. By using this command, users can receive suggestions for optimizing their code, which can lead to improved execution times and resource utilization.
# MAGIC
# MAGIC ## How to Use /optimize
# MAGIC
# MAGIC To utilize the `/optimize` command, simply type it in a code cell followed by the code you want to optimize. The command analyzes the provided code and returns recommendations based on best practices and performance improvements.
# MAGIC
# MAGIC ### Example Usage
# MAGIC
# MAGIC Here’s an example of how to use the `/optimize` command in a code cell:
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH employee_data AS (  -- Added CTE for better readability
# MAGIC     SELECT 
# MAGIC         a.name, 
# MAGIC         b.department, 
# MAGIC         c.salary, 
# MAGIC         d.bonus, 
# MAGIC         e.project_name, 
# MAGIC         f.location 
# MAGIC     FROM 
# MAGIC         employees a 
# MAGIC     JOIN 
# MAGIC         departments b ON a.department_id = b.id 
# MAGIC     LEFT JOIN 
# MAGIC         salaries c ON a.id = c.employee_id 
# MAGIC     LEFT JOIN 
# MAGIC         bonuses d ON a.id = d.employee_id 
# MAGIC     JOIN 
# MAGIC         projects e ON a.project_id = e.id 
# MAGIC     LEFT JOIN 
# MAGIC         locations f ON e.location_id = f.id 
# MAGIC     WHERE 
# MAGIC         c.salary > 50000 
# MAGIC         AND (d.bonus IS NOT NULL OR e.project_name LIKE 'Project%') 
# MAGIC         AND f.location IN ('New York', 'San Francisco') 
# MAGIC ) 
# MAGIC SELECT * FROM employee_data  -- Selecting from CTE
# MAGIC WHERE 
# MAGIC     a.department_id IS NOT NULL  -- Added WHERE clause for partition column
# MAGIC     AND c.salary >= date_sub(current_date(), 30)  -- Added WHERE clause for date column
# MAGIC LIMIT 1000;  -- Added LIMIT to avoid returning too many rows

# COMMAND ----------

# MAGIC %md
# MAGIC # 8: Rename Cell with the /rename Command
# MAGIC
# MAGIC The `/rename` command in Databricks notebooks allows you to quickly change the title (name) of a cell using natural language. This is useful for organizing your notebook, making sections clearer, and improving overall readability.
# MAGIC
# MAGIC ## Purpose
# MAGIC - Organize your notebook by giving cells meaningful titles
# MAGIC - Make it easier to navigate and understand your workflow
# MAGIC - Quickly update cell names without manually editing them
# MAGIC
# MAGIC ## How to Use /rename
# MAGIC To rename a cell, simply type `/rename` followed by the new title in the cell you want to update. The AI Assistant will automatically change the cell's title to your specified name.
# MAGIC
# MAGIC ### Example Usage
# MAGIC Suppose you want to rename a cell to "Data Cleaning Step 1". In that cell, type:
# MAGIC

# COMMAND ----------

# DBTITLE 1,Employee Salary and Bonus Analysis for Selected Locatio ...
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     a.name, 
# MAGIC     b.department, 
# MAGIC     c.salary, 
# MAGIC     d.bonus, 
# MAGIC     e.project_name, 
# MAGIC     f.location 
# MAGIC FROM 
# MAGIC     employees a 
# MAGIC JOIN 
# MAGIC     departments b ON a.department_id = b.id 
# MAGIC LEFT JOIN 
# MAGIC     salaries c ON a.id = c.employee_id 
# MAGIC LEFT JOIN 
# MAGIC     bonuses d ON a.id = d.employee_id 
# MAGIC JOIN 
# MAGIC     projects e ON a.project_id = e.id 
# MAGIC LEFT JOIN 
# MAGIC     locations f ON e.location_id = f.id 
# MAGIC WHERE 
# MAGIC     c.salary > 50000 
# MAGIC     AND (d.bonus IS NOT NULL OR e.project_name LIKE 'Project%') 
# MAGIC     AND f.location IN ('New York', 'San Francisco') 
# MAGIC ORDER BY 
# MAGIC     a.name, 
# MAGIC     b.department, 
# MAGIC     c.salary DESC;

# COMMAND ----------

#More Commands in Chat Mode
