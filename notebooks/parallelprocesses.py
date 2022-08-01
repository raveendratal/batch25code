# Databricks notebook source
# MAGIC %fs ls /databricks-datasets/asa/airlines

# COMMAND ----------

# MAGIC %fs head dbfs:/databricks-datasets/asa/airlines/2007.csv

# COMMAND ----------

df = spark.read.csv("/databricks-datasets/asa/airlines",header=True)
df.write.format("delta").mode("overwrite").saveAsTable("airline_data")

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs ls /user/hive/warehouse/airline_data/

# COMMAND ----------

print(16156639/1024/1024)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from airline_data
