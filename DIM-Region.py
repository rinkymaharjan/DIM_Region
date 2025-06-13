# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from datetime import date
from pyspark.sql.functions import (col, when, sum, upper, hash)

# COMMAND ----------

spark = SparkSession.builder.appName('Dim-Region').getOrCreate()

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

df_Region = df_Fact_Sales.select([
    col("Region")
]).distinct()

# COMMAND ----------

df_Region.display()

# COMMAND ----------

df_RegionHash = df_Region.withColumn("DIM-RegionID", hash(upper(col("Region"))).cast("bigint"))

# COMMAND ----------

dfDimFinal.display()

# COMMAND ----------

dfBase = spark.createDataFrame([
    ("N/A", -1)
], ["Region", "Dim-RegionID"])

dfDimFinal = df_RegionHash.union(dfBase)

# COMMAND ----------

dfDimFinal.write.format("delta").mode("overwrite").save("/FileStore/tables/DIM-Region")

# COMMAND ----------

df_DIMRegion = spark.read.format("delta").load("/FileStore/tables/DIM-Region")

# COMMAND ----------

df_DIMRegion.display()