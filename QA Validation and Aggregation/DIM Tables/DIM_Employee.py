# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, sha2, concat_ws)

# COMMAND ----------

spark = SparkSession.builder.appName("DIM_Employee").getOrCreate()

# COMMAND ----------

df_fact = spark.read.format("delta").load("/FileStore/tables/FactSalesDataV2")

# COMMAND ----------

df_fact.display()

# COMMAND ----------

DIM_Employee = df_fact.select([
    col("SalesRep"), col("Department"), col("EmployeeRole")
]).distinct()\
    .withColumn("EmployeeID", sha2(concat_ws("||", "SalesRep", "Department", "EmployeeRole"),256))

# COMMAND ----------

DIM_Employee.display()

# COMMAND ----------

DIM_Employee.write.format("delta").mode("overwrite").save("/FileStore/tables/DIM_Employee")

# COMMAND ----------

DIM_EmployeeFinal = spark.read.format("delta").load("/FileStore/tables/DIM_Employee")

# COMMAND ----------

DIM_EmployeeFinal.display()