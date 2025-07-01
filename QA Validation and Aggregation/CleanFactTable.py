# Databricks notebook source
# MAGIC %md
# MAGIC ## Fact table From CSV

# COMMAND ----------

from pyspark.sql import SparkSession
from datetime import date
from pyspark.sql.functions import (col, upper, trim,when, to_date)

# COMMAND ----------

spark = SparkSession.builder.appName("FactSalesDataV2").getOrCreate()

# COMMAND ----------

df_FactSalesData = spark.read.option("header", True).option("inferschema", True).csv("/FileStore/tables/fact_sales_data_v2.csv")

# COMMAND ----------

df_FactSalesData.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Replacing Nulls, Fixing Bad Values

# COMMAND ----------


df_CleanData = df_FactSalesData.select(
    trim(col("ProductCategory")).alias("ProductCategory"),
    when(col("ProductName").isNull(), "N/A").otherwise(trim(col("ProductName"))).alias("ProductName"),
    trim(col("Brand")).alias("Brand"),
    trim(col("StoreRegion")).alias("StoreRegion"),
    trim(col("StoreName")).alias("StoreName"),
    trim(col("StoreType")).alias("StoreType"),
    trim(col("SalesRep")).alias("SalesRep"),
    trim(col("Department")).alias("Department"),
    trim(col("EmployeeRole")).alias("EmployeeRole"),
    when(col("UnitsSold").isNull(), 0).otherwise(col("UnitsSold")).alias("UnitsSold"),
    when((col("UnitPrice").isNull()) | (col("UnitPrice") == -1), 0.0).otherwise(col("UnitPrice")).alias("UnitPrice"),
    when(col("Discount").isNull(), 0).otherwise(col("Discount")).alias("Discount"),
    to_date(col("SaleDate"), "yyyy-M-d").alias("SaleDate")
)


# COMMAND ----------

df_CleanData.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Saving Clean Data to Delta

# COMMAND ----------

df_CleanData.write.format("delta").mode("overwrite").save("/FileStore/tables/FactSalesDataV2")

# COMMAND ----------

df_Final = spark.read.format("delta").load("/FileStore/tables/FactSalesDataV2")

# COMMAND ----------

df_Final.display()