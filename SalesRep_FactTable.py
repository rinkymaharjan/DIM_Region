# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col)

# COMMAND ----------

spark = SparkSession.builder.appName('SalesRep').getOrCreate()

# COMMAND ----------

df_FactSales = spark.read.format("delta").load("/FileStore/tables/Facts_Sales")

df_SalesRep = spark.read.format("delta").load("/FileStore/tables/DIM-SalesRep")

# COMMAND ----------

Fact = df_FactSales.alias("f")
DIM = df_SalesRep.alias("sr")

df_joined = Fact.join( DIM, col("f.DIM-SalesRepID") == col("sr.SalesRepID"), how= "left")\
.select(col("f.UnitsSold"), col("f.Revenue"), col("f.DIM-SalesRepId"), col("sr.SalesRep"))


# COMMAND ----------

df_joined.display()

# COMMAND ----------

df_joined.write.format("delta").mode("overwrite").save("/FileStore/tables/SalesRepFactTable")

# COMMAND ----------

df_SalesRepFactTable = spark.read.format("delta").load("/FileStore/tables/SalesRepFactTable")

# COMMAND ----------

df_SalesRepFactTable.display()