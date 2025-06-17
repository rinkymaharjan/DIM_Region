# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col)

# COMMAND ----------

spark = SparkSession.builder.appName('ProductCategory').getOrCreate()

# COMMAND ----------

df_FactSales = spark.read.format("delta").load("/FileStore/tables/Facts_Sales")

df_DIMProductCategory = spark.read.format("delta").load("/FileStore/tables/DIM-ProductCategory")

# COMMAND ----------

Fact = df_FactSales.alias("f")
DIM = df_DIMProductCategory.alias("p")

df_joined = Fact.join( DIM, col("f.DIM-ProductCategoryID") == col("p.DIM-ProductCategoryID"), how= "left")\
.select(col("f.UnitsSold"), col("f.Revenue"), col("f.DIM-ProductCategoryId"), col("p.ProductCategory"))


# COMMAND ----------

df_joined.display()

# COMMAND ----------

df_joined.write.format("delta").mode("overwrite").save("/FileStore/tables/ProductCategoryFactTable")

# COMMAND ----------

df_ProductCategoryFact = spark.read.format("delta").load("/FileStore/tables/ProductCategoryFactTable")

# COMMAND ----------

df_ProductCategoryFact.display()