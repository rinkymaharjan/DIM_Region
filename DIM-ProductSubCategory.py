# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from datetime import date
from pyspark.sql.functions import (col, hash, upper)

# COMMAND ----------

spark = SparkSession.builder.appName("DIM-ProductSubCategory").getOrCreate()

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

df_ProductSubCategory = df_Fact_Sales.select([
    col("ProductSubCategory")
]).distinct()

# COMMAND ----------

df_ProductSubCategory.display()

# COMMAND ----------

df_ProductSubCategoryHash = df_ProductSubCategory.withColumn("DIM-ProductSubCategoryID", hash(upper(col("ProductSubCategory"))).cast("bigint"))

# COMMAND ----------

dfBase = spark.createDataFrame([
    ("N/A", -1)
], ["ProductSubCategory", "Dim-ProductSubCategoryID"])

dfDimFinal = df_ProductSubCategoryHash.union(dfBase)

# COMMAND ----------

dfBase

# COMMAND ----------

dfDimFinal.display()

# COMMAND ----------

dfDimFinal.write.format("delta").mode("overwrite").save("/FileStore/tables/DIM-ProductSubCategory")

# COMMAND ----------

df_DIMProductSubCategory = spark.read.format("delta").load("/FileStore/tables/DIM-ProductSubCategory")

# COMMAND ----------

df_DIMProductSubCategory.display()