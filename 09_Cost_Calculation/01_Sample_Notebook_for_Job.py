# Databricks notebook source
import time

print("Process started.")

start_time = time.time()
duration_minutes = 120

for minute in range(1, duration_minutes + 1):
    time.sleep(60)
    print(f"{minute} minutes passed.")

print("Process completed.")