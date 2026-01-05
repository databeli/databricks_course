# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction to Databricks Notebook Debug Mode
# MAGIC
# MAGIC Databricks notebook debug mode is a powerful feature that allows developers to troubleshoot and optimize their code effectively. This mode provides several key features that enhance the debugging process:
# MAGIC
# MAGIC ## Features
# MAGIC
# MAGIC - **Breakpoints**: You can set breakpoints in your code to pause execution at specific lines. This allows you to inspect the state of your program at critical points, making it easier to identify issues.
# MAGIC
# MAGIC - **Step Execution**: This feature enables you to execute your code line by line. By stepping through your code, you can observe how data changes and how control flows through your program, which is invaluable for understanding complex logic.
# MAGIC
# MAGIC - **Variable Inspection**: While in debug mode, you can inspect the values of variables at any point during execution. This helps you verify that your code is functioning as expected and allows you to identify any discrepancies in variable values.
# MAGIC
# MAGIC ## When to Use Debug Mode
# MAGIC
# MAGIC Debug mode is particularly useful in the following scenarios:
# MAGIC
# MAGIC - When you encounter unexpected behavior or errors in your code and need to identify the root cause.
# MAGIC - During the development of complex algorithms where understanding the flow of data is crucial.
# MAGIC - When optimizing code performance and needing to analyze how different parts of your code interact.
# MAGIC
# MAGIC By leveraging the debug mode in Databricks notebooks, you can enhance your coding efficiency and improve the quality of your data processing workflows.

# COMMAND ----------

# Example: Debugging a simple function in Databricks notebook

def calculate_average(numbers):
    """Calculate the average of a list of numbers."""
    total = 0
    for num in numbers:
        total += num  # You can set a breakpoint here to inspect 'total' and 'num'
    # Intentional bug: should be len(numbers), not len(total)
    average = total / len(numbers)  # Set a breakpoint here to see the error
    return average

# COMMAND ----------


# Test the function
numbers = [10, 20, 30, 40]
print("numbers=",numbers)
result = calculate_average(numbers)  # Set a breakpoint here to step into the function
print(f"Average: {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Using the Debugger with the Code
# MAGIC
# MAGIC To effectively debug the provided code, you can use a debugger tool available in most integrated development environments (IDEs) such as Visual Studio Code, PyCharm, or Jupyter Notebooks. Here’s a step-by-step guide on how to use the debugger:
# MAGIC
# MAGIC ## Setting Breakpoints
# MAGIC 1. **Open the Code**: Load your code in the IDE.
# MAGIC 2. **Set Breakpoints**: Click in the margin next to the line numbers where you want the execution to pause. A red dot will appear, indicating a breakpoint.
# MAGIC
# MAGIC ## Stepping Through the Code
# MAGIC 1. **Start Debugging**: Run the debugger by clicking the debug icon or pressing the designated shortcut (usually F5).
# MAGIC 2. **Step Over**: Use the "Step Over" command (often F10) to execute the current line and move to the next line without entering any functions.
# MAGIC 3. **Step Into**: Use the "Step Into" command (often F11) to go into a function call to see its execution.
# MAGIC 4. **Step Out**: If you are inside a function and want to return to the calling function, use the "Step Out" command (often Shift + F11).
# MAGIC
# MAGIC ## Inspecting Variables
# MAGIC - While the execution is paused at a breakpoint, hover over variables to see their current values.
# MAGIC - You can also use the "Variables" pane in the debugger to view and inspect all variables in the current scope.
# MAGIC
# MAGIC ## Expected Errors
# MAGIC - If there is a bug in the code, you might encounter errors such as `TypeError`, `ValueError`, or `IndexError`. The debugger will pause at the line where the error occurs, allowing you to inspect the state of the program.
# MAGIC
# MAGIC ## Fixing Bugs
# MAGIC - Once you identify the problematic line, you can modify the code accordingly. For example, if you encounter an `IndexError`, check the indices being accessed and ensure they are within the valid range.
# MAGIC
# MAGIC By following these steps, you can effectively debug the code, understand its flow, and fix any issues that arise.

# COMMAND ----------

