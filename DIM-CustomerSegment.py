# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from datetime import date
from pyspark.sql.functions import (col, upper, hash)

# COMMAND ----------

spark = SparkSession.builder.appName("DIM-CustomerSegment").getOrCreate()


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

df_CustomerSegment = df_Fact_Sales.select([
    col("CustomerSegment")
]).distinct()

# COMMAND ----------

df_CustomerSegment.display()

# COMMAND ----------

df_CustomerSegmentHash = df_CustomerSegment.withColumn("CustomerSegmentID", hash(upper(col("CustomerSegment"))).cast("bigint"))

# COMMAND ----------

dfBase = spark.createDataFrame([
    ("N/A", -1)
], ["CustomerSegment", "CustomerSegmentID"])

dfDimFinal = df_CustomerSegmentHash.union(dfBase)

# COMMAND ----------

dfDimFinal.display()

# COMMAND ----------

dfDimFinal.write.format("delta").mode("overwrite").save("/FileStore/tables/DIM-CustomerSegment")

# COMMAND ----------

df_DIMCustomerSegment = spark.read.format("delta").load("/FileStore/tables/DIM-CustomerSegment")

# COMMAND ----------

df_DIMCustomerSegment.display()