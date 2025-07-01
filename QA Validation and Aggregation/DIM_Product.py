# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, sha2, concat_ws)

# COMMAND ----------

spark = SparkSession.builder.appName("DIM_Product").getOrCreate()

# COMMAND ----------

df_Final = spark.read.format("delta").load("/FileStore/tables/FactSalesDataV2")

# COMMAND ----------

df_Final.display()

# COMMAND ----------

DIM_Product = df_Final.select([
    col("ProductCategory"), col("ProductName"), col("Brand")
]).distinct()\
    .withColumn("DIM_ProductID", sha2(concat_ws("||", "ProductCategory", "ProductName", "Brand"), 256))


# COMMAND ----------

DIM_Product.display()

# COMMAND ----------

DIM_Product.write.format("delta").mode("overwrite").save("/FileStore/tables/DIM_Product")

# COMMAND ----------

DIM_ProductFinal = spark.read.format("delta").load("/FileStore/tables/DIM_Product")

# COMMAND ----------

DIM_ProductFinal.display()