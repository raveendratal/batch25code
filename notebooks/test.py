# Databricks notebook source
df1 = spark.read.format("csv").option("header", "true").load("/databricks-datasets/asa/airlines")

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/asa/airlines

# COMMAND ----------

df1.count()