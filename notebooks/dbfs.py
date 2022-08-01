# Databricks notebook source
# MAGIC %fs ls /databricks-datasets

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets/"))

# COMMAND ----------

display(dbutils.fs.mounts())