# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col,sha2, concat_ws, trim, upper)

# COMMAND ----------

spark = SparkSession.builder.appName("MappingLabel").getOrCreate()

# COMMAND ----------

df_Customer = spark.read.option("header", True).option("inferschema", True).csv("/FileStore/tables/Dim_CustomerType.csv")

df_ProductCategory = spark.read.option("header", True).option("inferschema", True).csv("/FileStore/tables/Dim_ProductCategory.csv")

df_Store = spark.read.option("header", True).option("inferschema", True).csv("/FileStore/tables/Dim_StoreRegion.csv")

df_FactTransactions = spark.read.option("header", True).option("inferschema", True).csv("/FileStore/tables/Fact_Transactions.csv")

df_CustomMapping = spark.read.option("header", True).csv("/FileStore/tables/Custom_Mapping_DIM.csv")


# COMMAND ----------

DIM_CustomLabel = df_CustomMapping.withColumn("DIM_CustomLabelID", \
    sha2(concat_ws("||", upper(trim(col("ProductCategory"))), upper(trim(col("StoreRegion"))), upper(trim(col("CustomerType")))), 256))

# COMMAND ----------

DIM_CustomLabel.display()

# COMMAND ----------

df_FactKeys = df_FactTransactions\
.withColumn("Dim_ProductCategoryID", sha2(upper(trim(col("ProductCategory"))), 256))\
.withColumn("Dim_StoreRegionID", sha2(upper(trim(col("StoreRegion"))), 256))\
.withColumn("Dim_CustomerTypeID", sha2(upper(trim(col("CustomerType"))), 256))\
.withColumn("DIM_CustomLabelID", sha2(concat_ws("||",
        upper(trim(col("ProductCategory"))),
        upper(trim(col("StoreRegion"))),
        upper(trim(col("CustomerType")))), 256))

# COMMAND ----------

df_join = df_FactKeys.alias("f").join(DIM_CustomLabel\
               .select("ProductCategory", "StoreRegion", "CustomerType", "MappingLabel").alias("c"), \
                   (["ProductCategory", "StoreRegion", "CustomerType"]), "left")

# COMMAND ----------

df_Final = df_join.select("f.TransactionID", "f.DIM_ProductCategoryID", "f.DIM_StoreRegionID", "f.DIM_CustomerTypeID",
"f.DIM_CustomLabelID", "c.MappingLabel", "f.Amount")

# COMMAND ----------

df_Final.display()

# COMMAND ----------

df_Final.write.format("delta").mode("overwrite").save("/FileStore/tables/MappingLabel")

# COMMAND ----------

