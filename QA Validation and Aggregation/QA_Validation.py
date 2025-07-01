# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, sha2, concat_ws)

# COMMAND ----------

spark = SparkSession.builder.appName("QA_Validation").getOrCreate()

# COMMAND ----------

df_Val = spark.read.format("delta").load("/FileStore/tables/FactWithKeys")

df_Final = spark.read.format("delta").load("/FileStore/tables/FactSalesDataV2")

# COMMAND ----------

df_Final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####### QA Validation. If there are any mismatches then the data is incorrect in net revenue.

# COMMAND ----------


df_check = df_Val.withColumn("ExpectedNetRevenue",
    col("UnitsSold") * col("UnitPrice") * (1 - col("Discount") / 100))

df_check.filter(col("NetRevenue") != col("ExpectedNetRevenue")).count()
