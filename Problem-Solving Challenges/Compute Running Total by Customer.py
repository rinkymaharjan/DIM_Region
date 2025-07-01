# Databricks notebook source
# MAGIC %md
# MAGIC ### Challenge 4: Compute Running Total by Customer
# MAGIC
# MAGIC Use a window function to compute cumulative sum of purchases per customer.

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import (col, sum)

data = [
    Row(customer_id=1, date='2023-01-01', amount=100),
    Row(customer_id=1, date='2023-01-02', amount=200),
    Row(customer_id=2, date='2023-01-01', amount=300),
    Row(customer_id=2, date='2023-01-02', amount=400)
]
df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

df_partition = Window.partitionBy("customer_id").orderBy("date")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### Here grouping by "customer_id" column and ordering by date column using window function.

# COMMAND ----------

df_CummulativeSum = df.withColumn("Cumulative_sum", sum("amount").over(df_partition))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### creating new column named "cumulative_sum" which stores the value that is sum of the "amount" column over dataframe df_partition which is a dataframe partioned by customer_id and oderder by date column.  

# COMMAND ----------

df_CummulativeSum.display()

# COMMAND ----------

df_CummulativeSum. write.format("delta").mode("overwrite").save("/FileStore/tables/df_CummulativeSum")