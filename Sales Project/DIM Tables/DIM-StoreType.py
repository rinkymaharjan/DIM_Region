# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from datetime import date
from pyspark.sql.functions import (col, upper, hash)

# COMMAND ----------

spark = SparkSession.builder.appName("DIM-StoreType").getOrCreate()

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

df_StoreType = df_Fact_Sales.select([
    col("StoreType")
]).distinct()

# COMMAND ----------

df_StoreType.display()

# COMMAND ----------

df_StoreTypeHash = df_StoreType.withColumn("StoreTypeID", hash(upper(col("StoreType"))).cast("bigint"))


# COMMAND ----------

dfBase = spark.createDataFrame([
    ("N/A", -1)
], ["StoreType", "StoreTypeID"])

dfDimFinal = df_StoreTypeHash.union(dfBase)

# COMMAND ----------

dfDimFinal.display()

# COMMAND ----------

dfDimFinal.write.format("delta").mode("overwrite").save("/FileStore/tables/DIM-StoreType")

# COMMAND ----------

df_DIMStoreType = spark.read.format("delta").load("/FileStore/tables/DIM-StoreType")

# COMMAND ----------

df_DIMStoreType.display()