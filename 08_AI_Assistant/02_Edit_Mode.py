# Databricks notebook source
# MAGIC %md
# MAGIC # Edit Mode Features: Quick Overview
# MAGIC
# MAGIC Good morning!
# MAGIC
# MAGIC This notebook introduces new users to the powerful features of edit mode in the Databricks assistant . Each section provides a clear explanation and example of how edit mode can help you:
# MAGIC
# MAGIC - **Contextual Assistance:** Receive help tailored to your cloud, workspace, and coding context.
# MAGIC - **Code Completion:** Get intelligent suggestions to finish your code.
# MAGIC - **Error Correction:** Detect and fix errors automatically.
# MAGIC - **Code Explanation:** Understand what your code does with clear explanations.
# MAGIC - **Code Optimization:** Improve performance and efficiency with smart suggestions.
# MAGIC - **Documentation & Formatting:** Generate comments, docstrings, and format code for readability.
# MAGIC
# MAGIC
# MAGIC Each feature is demonstrated with practical Python code and markdown explanations, making it easy for you to learn and apply these capabilities in your own projects.
# MAGIC
# MAGIC Good morning and happy coding!

# COMMAND ----------

# MAGIC %md
# MAGIC # 1: Code Completion Example 
# MAGIC

# COMMAND ----------

# Calculate the sum of squares for numbers 1 to 5
numbers = [1, 2, 3, 4, 5]  # List of numbers
sum_of_squares = 0  # Initialize sum accumulator
for num in numbers:
    # Add the square of the current number to the sum
    sum_of_squares += num ** 2
print("Sum of squares:", sum_of_squares)  # Output the result

# COMMAND ----------

# MAGIC %md
# MAGIC # 2: Error Correction Example 

# COMMAND ----------

# Create a Spark DataFrame with sample employee data
# Columns: firstname, lastname, gender, age, salary
df = spark.createDataFrame([
    ('Alice', 'Smith', 'Female', 30, 50000),
    ('Bob', 'Johnson', 'Male', 40, 60000),
    ('Charlie', 'Williams', 'Male', 25, 45000),
    ('David', 'Brown', 'Male', 35, 55000),
    ('Eve', 'Davis', 'Female', 28, 48000),
    ('Frank', 'Miller', 'Male', 32, 52000),
    ('Grace', 'Taylor', 'Female', 38, 58000),
    ('Henry', 'Anderson', 'Male', 45, 62000),
    ('Ivy', 'White', 'Female', 22, 42000),
    ('Jack', 'Harris', 'Male', 33, 53000)
], ['firstname', 'lastname', 'gender', 'age', 'salary'])

# Group by gender and calculate the average salary for each group
df_filtered = df.groupBy('gender').agg({'salary': 'avg'})

# COMMAND ----------

# Display the result of the groupBy aggregation
# Shows average salary by gender
display(df_filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC # 3: Explain the code

# COMMAND ----------

# Code snippet: Find even numbers in a list
numbers = [1, 2, 3, 4, 5, 6]  # List of numbers
# Use list comprehension to filter even numbers
even_numbers = [num for num in numbers if num % 2 == 0]
print("Even numbers:", even_numbers)  # Output the result

# COMMAND ----------

# MAGIC %md
# MAGIC # 4: Optimize the code

# COMMAND ----------

# Unoptimized code: Find the sum of even numbers in a large list
numbers = list(range(1, 10001))  # Create a list of numbers from 1 to 10,000
sum_even = 0  # Initialize sum accumulator
for num in numbers:
    if num % 2 == 0:  # Check if the number is even
        sum_even += num  # Add even number to the sum
print("Sum of even numbers:", sum_even)  # Output the result

# COMMAND ----------

# MAGIC %md
# MAGIC # 5: Documentation the code better

# COMMAND ----------

# This cell demonstrates how to add comments and markdown documentation in your notebook.
# Comments in code cells help explain logic and improve readability.
# You can also add or edit Markdown cells to provide context, instructions, or explanations for your code.
# Example:
# - Use '#' for comments in Python code cells.
# - Use Markdown cells for formatted text, headings, and documentation.