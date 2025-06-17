# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col)

# COMMAND ----------

spark = SparkSession.builder.appName('StoreType').getOrCreate()

# COMMAND ----------

df_FactSales = spark.read.format("delta").load("/FileStore/tables/Facts_Sales")

df_StoreType = spark.read.format("delta").load("/FileStore/tables/DIM-StoreType")

# COMMAND ----------

Fact = df_FactSales.alias("f")
DIM = df_StoreType.alias("st")

df_joined = Fact.join( DIM, col("f.DIM-StoreTypeID") == col("st.StoreTypeID"), how= "left")\
.select(col("f.UnitsSold"), col("f.Revenue"), col("f.DIM-StoreTypeId"), col("st.StoreType"))


# COMMAND ----------

df_joined.display()

# COMMAND ----------

df_joined.write.format("delta").mode("overwrite").save("/FileStore/tables/StoreTypeFactTable")

# COMMAND ----------

df_StoreTypeFactTable = spark.read.format("delta").load("/FileStore/tables/StoreTypeFactTable")

# COMMAND ----------

df_StoreTypeFactTable.display()