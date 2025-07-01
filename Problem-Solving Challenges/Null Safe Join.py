# Databricks notebook source
# MAGIC %md
# MAGIC ### Challenge 10: Null Safe Join
# MAGIC
# MAGIC Join two datasets where join key might have nulls, handle using null-safe join.

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import (expr, col)

data1 = [
    Row(id=1, name='John'),
    Row(id=None, name='Mike'),
    Row(id=2, name='Alice')
]
data2 = [
    Row(id=1, salary=5000),
    Row(id=None, salary=3000)
]
df1 = spark.createDataFrame(data1)
df2 = spark.createDataFrame(data2)
df1.show()
df2.show()

# COMMAND ----------

df_join = df1.alias("a").join(df2.alias("b"), expr("a.id <=> b.id"), "left")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### Joining two dataframes df1 using alias "a" and df2 using alias "b" and using inner join on same id on both dataframes. Here we are using <=> that implies that df1 dataframe is equal to df2 dataframe even if both are null. its logic implies null<=> null is true which creates null safe join.

# COMMAND ----------

df_Final = df_join.select(col("a.id").alias("id"), "name", "salary")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Selecting only one column with "id" with name and salary.

# COMMAND ----------

df_Final.display()

# COMMAND ----------

df_Final.write.format("delta").mode("overwrite").save("/FileStore/tables/df_Final")