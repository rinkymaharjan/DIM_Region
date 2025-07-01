# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from datetime import date, timedelta
from pyspark.sql.functions import (col, upper, hash, to_date, trim, date_format, year, month, quarter, expr, lit, when, concat)




# COMMAND ----------

spark = SparkSession.builder.appName("DIM-SalesDate").getOrCreate()

# COMMAND ----------

Schema = StructType([
    StructField("Region", StringType(), True),
    StructField("ProductCategory", StringType(), True),
    StructField("ProductSubCategory", StringType(), True),
    StructField("SalesChannel", StringType(), True),
    StructField("CustomerSegment", StringType(), True),
    StructField("SalesRep", StringType(), True),
    StructField("StoreType", StringType(), True),
    StructField("SalesDate", StringType(), True),
    StructField("UnitsSold", IntegerType(), True),
    StructField("Revenue", IntegerType(), True)
])

df_Fact_Sales = spark.read.option("header", True).schema(Schema).csv("/FileStore/tables/fact_sales.csv")

# COMMAND ----------


df_Fact_Sales = df_Fact_Sales.withColumn("SalesDate", to_date(trim("SalesDate"), "M/d/yyyy"))

# COMMAND ----------

df_Fact_Sales.display()

# COMMAND ----------

df_Fact_Sales.write.format("delta").mode("overwrite").save("/FileStore/tables/Fact_Sales")

# COMMAND ----------

df_FactSales = spark.read.format("delta").load("/FileStore/tables/Fact_Sales")

# COMMAND ----------

df_Fact_Sales.display()