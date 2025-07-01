# Databricks notebook source
# MAGIC %md
# MAGIC ### Challenge 2: Highest Transaction Per Day
# MAGIC
# MAGIC Find the highest transaction amount for each day.

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import (max)

data = [
    Row(date='2023-01-01', amount=100),
    Row(date='2023-01-01', amount=300),
    Row(date='2023-01-02', amount=150),
    Row(date='2023-01-02', amount=200)
]
df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

Highest_Transaction = df.groupBy("date").agg(max("amount").alias("HighestTransaction"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### I created a new dataframe called "Highest_Transaction" which stores max("amount") showing highest amount. Giving aprropriate alias to the newly created column with .alias("HighestTransaction"). And grouping by "date" gives highest transaction per day. 

# COMMAND ----------

Highest_Transaction.display()

# COMMAND ----------

Highest_Transaction.write.format("delta").mode("overwrite").save("/FileStore/tables/Highest_Transaction")

# COMMAND ----------

Highest_Transaction_Per_Day = spark.read.format("delta").load("/FileStore/tables/Highest_Transaction")

# COMMAND ----------

Highest_Transaction_Per_Day.display()