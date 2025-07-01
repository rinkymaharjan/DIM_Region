# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col)

# COMMAND ----------

spark = SparkSession.builder.appName('SalesDate').getOrCreate()

# COMMAND ----------

df_FactSales = spark.read.format("delta").load("/FileStore/tables/Facts_Sales")

df_DIMSalesDate = spark.read.format("delta").load("/FileStore/tables/DIM-SalesDate")

# COMMAND ----------

Fact = df_FactSales.alias("f")
DIM = df_DIMSalesDate.alias("dd")

df_joined = Fact.join( DIM, col("f.DIM-DateID") == col("dd.DIM_DateID"), how= "left")\
.select(col("f.UnitsSold"), col("f.Revenue"), col("f.DIM-DateID"), col("dd.Date"))


# COMMAND ----------

df_joined.display()

# COMMAND ----------

df_joined.write.format("delta").mode("overwrite").save("/FileStore/tables/SalesDate")

# COMMAND ----------

df_SalesDateFactTable = spark.read.format("delta").load("/FileStore/tables/SalesDate")

# COMMAND ----------

df_SalesDateFactTable.display()