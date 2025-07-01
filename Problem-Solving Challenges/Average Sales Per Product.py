# Databricks notebook source
# MAGIC %md
# MAGIC ### Challenge 5: Average Sales Per Product
# MAGIC
# MAGIC Find average amount per product.

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import (avg)

data = [
    Row(product='A', amount=100),
    Row(product='B', amount=200),
    Row(product='A', amount=300),
    Row(product='B', amount=400)
]
df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

Avg_Sales = df.groupBy("product").agg(avg("amount").alias("AverageAmount"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### I created a new dataframe called "Avg_Sales" which stores avg("amount") showing average amount. Giving appropriate alias to the newly created column with .alias("AverageAmount"). And grouping by "product" gives average sales per product.

# COMMAND ----------

Avg_Sales.display()

# COMMAND ----------

Avg_Sales.write.format("delta").mode("overwrite").save("/FileStore/tables/Avg_Sales")