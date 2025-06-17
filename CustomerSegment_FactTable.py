# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col)

# COMMAND ----------

spark = SparkSession.builder.appName('CustomerSegment').getOrCreate()

# COMMAND ----------

df_FactSales = spark.read.format("delta").load("/FileStore/tables/Facts_Sales")

df_DIMCustomerSegment = spark.read.format("delta").load("/FileStore/tables/DIM-CustomerSegment")

# COMMAND ----------

Fact = df_FactSales.alias("f")
DIM = df_DIMCustomerSegment.alias("cus")

df_joined = Fact.join( DIM, col("f.DIM-CustomerSegmentID") == col("cus.CustomerSegmentID"), how= "left")\
.select(col("f.UnitsSold"), col("f.Revenue"), col("f.DIM-CustomerSegmentId"), col("cus.CustomerSegment"))


# COMMAND ----------

df_joined.display()

# COMMAND ----------

df_joined.write.format("delta").mode("overwrite").save("/FileStore/tables/CustomerSegmentFactTable")

# COMMAND ----------

df_CustomerSegmentFactTable = spark.read.format("delta").load("/FileStore/tables/CustomerSegmentFactTable")

# COMMAND ----------

df_CustomerSegmentFactTable.display()