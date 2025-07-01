# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col)

# COMMAND ----------

spark = SparkSession.builder.appName('SalesChannel').getOrCreate()

# COMMAND ----------

df_FactSales = spark.read.format("delta").load("/FileStore/tables/Facts_Sales")

df_DIMSalesChannel = spark.read.format("delta").load("/FileStore/tables/DIM-SalesChannel")

# COMMAND ----------

Fact = df_FactSales.alias("f")
DIM = df_DIMSalesChannel.alias("sc")

df_joined = Fact.join( DIM, col("f.DIM-SalesChannelID") == col("sc.DIM-df_SalesChannelID"), how= "left")\
.select(col("f.UnitsSold"), col("f.Revenue"), col("f.DIM-SalesChannelId"), col("sc.SalesChannel"))


# COMMAND ----------

df_joined.display()

# COMMAND ----------

df_joined.write.format("delta").mode("overwrite").save("/FileStore/tables/SalesChannelFactTable")

# COMMAND ----------

df_SalesChannelFactTable = spark.read.format("delta").load("/FileStore/tables/SalesChannelFactTable")

# COMMAND ----------

df_SalesChannelFactTable.display()