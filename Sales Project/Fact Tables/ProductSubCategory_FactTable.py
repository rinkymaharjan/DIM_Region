# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col)

# COMMAND ----------

spark = SparkSession.builder.appName('ProductSubCategory').getOrCreate()

# COMMAND ----------

df_FactSales = spark.read.format("delta").load("/FileStore/tables/Facts_Sales")

df_DIMProductSubCategory = spark.read.format("delta").load("/FileStore/tables/DIM-ProductSubCategory")

# COMMAND ----------

Fact = df_FactSales.alias("f")
DIM = df_DIMProductSubCategory.alias("s")

df_joined = Fact.join( DIM, col("f.DIM-ProductCategoryID") == col("s.DIM-ProductSubCategoryID"), how= "left")\
.select(col("f.UnitsSold"), col("f.Revenue"), col("f.DIM-ProductSubCategoryId"), col("s.ProductSubCategory"))


# COMMAND ----------

df_joined.display()

# COMMAND ----------

df_joined.write.format("delta").mode("overwrite").save("/FileStore/tables/ProductSubCategoryFactTable")

# COMMAND ----------

df_ProductSubCategoryFact = spark.read.format("delta").load("/FileStore/tables/ProductSubCategoryFactTable")

# COMMAND ----------

df_ProductSubCategoryFact.display()