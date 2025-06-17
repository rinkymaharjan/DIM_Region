# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from datetime import date
from pyspark.sql.functions import (col, upper, hash)

# COMMAND ----------

spark = SparkSession.builder.appName('DIM-ProductCategory').getOrCreate()


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

df_ProductCategory = df_Fact_Sales.select([
    col("ProductCategory")
]).distinct()

# COMMAND ----------

df_ProductCategory.display()

# COMMAND ----------

df_ProductCategoryHash = df_ProductCategory.withColumn("DIM-ProductCategoryID", hash(upper(col("ProductCategory"))).cast("bigint"))

# COMMAND ----------

dfBase = spark.createDataFrame([
    ("N/A", -1)
], ["ProductCategory", "Dim-ProductCategoryID"])

dfDimFinal = df_ProductCategoryHash.union(dfBase)

# COMMAND ----------

dfDimFinal.display()

# COMMAND ----------

dfDimFinal.write.format("delta").mode("overwrite").save("/FileStore/tables/DIM-ProductCategory")

# COMMAND ----------

df_DIMProductCategory = spark.read.format("delta").load("/FileStore/tables/DIM-ProductCategory")

# COMMAND ----------

df_DIMProductCategory.display()