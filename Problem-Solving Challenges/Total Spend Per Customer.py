# Databricks notebook source
# MAGIC %md
# MAGIC ### Challenge 1: Total Spend Per Customer
# MAGIC
# MAGIC Calculate total amount spent by each customer.
# MAGIC

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import (sum)

data = [
    Row(customer_id=1, amount=250),
    Row(customer_id=2, amount=450),
    Row(customer_id=1, amount=100),
    Row(customer_id=3, amount=300),
    Row(customer_id=2, amount=150)
]
df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

Total_Spend = df.groupBy("customer_id").agg(sum("amount").alias("TotalSpend"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC > ##### I created a new dataframe called "Total_Spend" which stores sum("amount") giving total amount spent. Giving aprropriate alias to the newly created column with .alias("TotalSpend"). And grouping by customer_id gives total amount spent by each customer. 

# COMMAND ----------

Total_Spend.display()

# COMMAND ----------

Total_Spend.write.format("delta").mode("overwrite").save("/FileStore/tables/Total_Spend")

# COMMAND ----------

Total_Spend_By_Customer = spark.read.format("delta").load("/FileStore/tables/Total_Spend") 

# COMMAND ----------

Total_Spend_By_Customer.display()