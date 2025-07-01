# Databricks notebook source
# MAGIC %md
# MAGIC ### Challenge 6: Extract Year From Date
# MAGIC
# MAGIC Add a column to extract year from given date.
# MAGIC

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import (year)

data = [
    Row(customer='John', transaction_date='2022-11-01'),
    Row(customer='Alice', transaction_date='2023-01-01')
]
df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

df_Year = df.withColumn("Year", year("transaction_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Using withColumn to create a new column and named it "Year", using year function which was imported from spark functions. And getting only year from "transaction_date" column.

# COMMAND ----------

df_Year.display()

# COMMAND ----------

df_Year.write.format("delta").mode("overwrite").save("/FileStore/tables/df_Year")

# COMMAND ----------

year = spark.read.format("delta").load("/FileStore/tables/df_Year")

# COMMAND ----------

year.display()