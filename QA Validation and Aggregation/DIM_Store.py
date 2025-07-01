# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, sha2, concat_ws)

# COMMAND ----------

spark = SparkSession.builder.appName("DIM_Store").getOrCreate()

# COMMAND ----------

df_facttable = spark.read.format("delta").load("/FileStore/tables/FactSalesDataV2")

# COMMAND ----------

df_facttable.display()

# COMMAND ----------

DIM_Store = df_facttable.select([
    col("StoreRegion"), col("StoreName"), col("StoreType")
]).distinct()\
    .withColumn("DIM_StoreID", sha2(concat_ws("||", "StoreRegion", "StoreName", "StoreType"),256))

# COMMAND ----------

DIM_Store.display()

# COMMAND ----------

DIM_Store.write.format("delta").mode("overwrite").save("/FileStore/tables/DIM_Store")

# COMMAND ----------

df_DIMStore_Final = spark.read.format("delta").load("/FileStore/tables/DIM_Store")

# COMMAND ----------

df_DIMStore_Final.display()