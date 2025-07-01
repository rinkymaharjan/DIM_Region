# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, round, sum, broadcast)

# COMMAND ----------

spark =  SparkSession.builder.appName("JoinAndAgg").getOrCreate()

# COMMAND ----------

df_Final = spark.read.format("delta").load("/FileStore/tables/FactSalesDataV2")
DIM_Product = spark.read.format("delta").load("/FileStore/tables/DIM_Product")
DIM_Store = spark.read.format("delta").load("/FileStore/tables/DIM_Store")
DIM_Employee = spark.read.format("delta").load("/FileStore/tables/DIM_Employee")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using Broadcast Join

# COMMAND ----------

df_NewFact = df_Final.join(broadcast(DIM_Product), ["ProductCategory", "ProductName", "Brand"], "left")\
    .join(broadcast(DIM_Store), ["StoreRegion", "StoreName", "StoreType"], "left")\
    .join(broadcast(DIM_Employee), ["SalesRep", "Department", "EmployeeRole"], "left")\
    .select("DIM_ProductID","DIM_StoreID", "EmployeeID","UnitsSold", "UnitPrice", "Discount","SaleDate")


# COMMAND ----------

df_NewFact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aggregation

# COMMAND ----------

CleanFact_WithKeys = spark.read.format("delta").load("/FileStore/tables/FactWithKeys")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Total revenue by Region

# COMMAND ----------

df_RegionRevenue = CleanFact_WithKeys.join(DIM_Store, ["DIM_StoreID"])
df_RegionRevenue.groupBy("StoreRegion").agg(sum("NetRevenue").alias("TotalRevenue")).display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Total revenue by SalesRep

# COMMAND ----------

df_RevByRep = CleanFact_WithKeys.join(DIM_Employee, ["EmployeeID"])
df_RevByRep.groupBy("SalesRep").agg(sum("NetRevenue").alias("TotalRevenue")).display()