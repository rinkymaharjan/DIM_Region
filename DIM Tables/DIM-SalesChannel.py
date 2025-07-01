# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from datetime import date
from pyspark.sql.functions import (col, upper, hash)

# COMMAND ----------

spark = SparkSession.builder.appName('DIM-SalesChannel').getOrCreate()

# COMMAND ----------

Schema = StructType([
    StructField("Region", StringType(), True),
    StructField("ProductCategory", StringType(), True),
    StructField("ProductSubCategory", StringType(), True),
    StructField("SalesChannel", StringType(), True),
    StructField("CustomerSegment", StringType(), True),
    StructField("SalesRep", StringType(), True),
    StructField("StoreType", StringType(), True),
    StructField("SalesDate", DateType(), True),
    StructField("UnitsSold", IntegerType(), True),
    StructField("Revenue", IntegerType(), True)
])

df_Fact_Sales = spark.read.option("header", True).schema(Schema).csv("/FileStore/tables/fact_sales.csv")

# COMMAND ----------

df_SalesChannel = df_Fact_Sales.select([
    col("SalesChannel")
]).distinct()

# COMMAND ----------

df_SalesChannel.display()

# COMMAND ----------

df_SalesChannelHash = df_SalesChannel.withColumn("DIM-df_SalesChannelID", hash(upper(col("SalesChannel"))).cast("bigint"))

# COMMAND ----------

dfBase = spark.createDataFrame([
    ("N/A", -1)
], ["SalesChannel", "Dim-SalesChannelID"])

dfDimFinal = df_SalesChannelHash.union(dfBase)

# COMMAND ----------

dfDimFinal.display()

# COMMAND ----------

dfDimFinal.write.format("delta").mode("overwrite").save("/FileStore/tables/DIM-SalesChannel")

# COMMAND ----------

df_DIMSalesChannel = spark.read.format("delta").load("/FileStore/tables/DIM-SalesChannel")

# COMMAND ----------

df_DIMSalesChannel.display()