# Databricks notebook source
# MAGIC %md
# MAGIC ### Challenge 9: Top-N Records Per Group
# MAGIC
# MAGIC For each category, return top 2 records based on score.

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql.functions import(col, row_number)

data = [
    Row(category='A', name='x', score=80),
    Row(category='A', name='y', score=90),
    Row(category='A', name='z', score=70),
    Row(category='B', name='p', score=60),
    Row(category='B', name='q', score=85)
]
df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

df_partition = Window.partitionBy(col("category")).orderBy(col("score").desc())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Using Window.partitionBy(col("category")) to group by category and ordering by score column in descending order.

# COMMAND ----------

df_rownum = df.withColumn("row_num", row_number().over(df_partition))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### creating new column called "row_num" which stores rank using row_number function over df_partition dataframe. 

# COMMAND ----------

df_rownum.display()

# COMMAND ----------

Top2Records = df_rownum.filter(col("row_num") <=2 )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### Here we are using filter function to return only those rows where the column "row_num" is <=2. 

# COMMAND ----------

Top2Records.display()

# COMMAND ----------

Top2Records.write.format("delta").mode("overwrite").save("/FileStore/tables/Top2Records")