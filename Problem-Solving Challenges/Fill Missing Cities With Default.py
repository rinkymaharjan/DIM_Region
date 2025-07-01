# Databricks notebook source
# MAGIC %md
# MAGIC ### Challenge 3: Fill Missing Cities With Default
# MAGIC
# MAGIC Replace null city values with 'Unknown'.

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import (when, col)

data = [
    Row(customer_id=1, city='Dallas'),
    Row(customer_id=2, city=None),
    Row(customer_id=3, city='Austin'),
    Row(customer_id=4, city=None)
]
df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

Cities_Default = df.withColumn("city", when(col("city").isNull(), "Unknown").otherwise(col("city")))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### withColumn "city" will update the column. with condition- when the column "city" has any value that is null then replace it with "Unknown" otherwise just use whatever value is inside the city column. 

# COMMAND ----------

Cities_Default.display()

# COMMAND ----------

Cities_Default.write.format("delta").mode("overwrite").save("/FileStore/tables/Cities_Default")