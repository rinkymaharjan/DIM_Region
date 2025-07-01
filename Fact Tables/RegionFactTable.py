# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col)

# COMMAND ----------

spark = SparkSession.builder.appName('Region').getOrCreate()

# COMMAND ----------

df_FactSales = spark.read.format("delta").load("/FileStore/tables/Facts_Sales")

df_DIMRegion = spark.read.format("delta").load("/FileStore/tables/DIM-Region")

# COMMAND ----------

Fact = df_FactSales.alias("f")
DIM = df_DIMRegion.alias("d")

df_joined = Fact.join( DIM, col("f.`DIM-RegionId`") == col("d.`DIM-RegionID`"), how= "left")\
.select(col("f.UnitsSold"), col("f.Revenue"), col("f.DIM-RegionId"), col("d.Region"))


# COMMAND ----------

df_joined.display()

# COMMAND ----------

df_joined.write.format("delta").mode("overwrite").save("/FileStore/tables/RegionFactTable")

# COMMAND ----------

df_RegionFact = spark.read.format("delta").load("/FileStore/tables/RegionFactTable")

# COMMAND ----------

df_RegionFact.display()