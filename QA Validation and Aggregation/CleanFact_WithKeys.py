# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import(col, expr)

# COMMAND ----------

spark = SparkSession.builder.appName("FactWithKeys").getOrCreate()

# COMMAND ----------

df_Fact = spark.read.format("delta").load("/FileStore/tables/FactSalesDataV2")
DIM_Product = spark.read.format("delta").load("/FileStore/tables/DIM_Product")
DIM_Store = spark.read.format("delta").load("/FileStore/tables/DIM_Store")
DIM_Employee = spark.read.format("delta").load("/FileStore/tables/DIM_Employee")

# COMMAND ----------

New_Fact = df_Fact.join(DIM_Product, ["ProductCategory", "ProductName", "Brand"], how = "left")\
    .join(DIM_Store, ["StoreRegion", "StoreName", "StoreType"], how = "left")\
    .join(DIM_Employee, ["SalesRep", "Department", "EmployeeRole"], how = "left")\
    .select("DIM_ProductID","DIM_StoreID", "EmployeeID","UnitsSold", "UnitPrice", "Discount","SaleDate")

# COMMAND ----------

Fact_Revenue = New_Fact.withColumn("NetRevenue",
    expr("UnitsSold * UnitPrice * (1 - Discount/100)"))

# COMMAND ----------

Fact_Revenue.display()

# COMMAND ----------

Fact_Revenue.write.format("delta").mode("overwrite").save("/FileStore/tables/FactWithKeys")

# COMMAND ----------

CleanFact_WithKeys = spark.read.format("delta").load("/FileStore/tables/FactWithKeys")

# COMMAND ----------

CleanFact_WithKeys.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Validation

# COMMAND ----------


CleanFact_WithKeys.filter("DIM_ProductID IS NULL").count()

CleanFact_WithKeys.filter("DIM_StoreID IS NULL").count()

CleanFact_WithKeys.filter("EmployeeID IS NULL").count()
