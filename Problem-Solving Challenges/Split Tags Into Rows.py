# Databricks notebook source
# MAGIC %md
# MAGIC ### Challenge 8: Split Tags Into Rows
# MAGIC
# MAGIC Given a list of comma-separated tags, explode them into individual rows.

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import(col, split, explode)

data = [
    Row(id=1, tags='tech,news'),
    Row(id=2, tags='sports,music'),
    Row(id=3, tags='food')
]
df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

df_Split = df.withColumn("Split_Tag", split(col("tags"), ","))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Creating a new column "Split_Tag" and using split function to convert the string to a list separated by ",". 

# COMMAND ----------

df_Split.display()

# COMMAND ----------

df_NewTagRows = df_Split.select( col("id"), explode(col("Split_Tag")).alias("tags"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### Selecting to show the "id" and "Split_Tag" from df_Split dataframe and changing the name to tags which replaces the existing "tags" column with newly updated values. Using explode function in column "Split_Tag" creates new row for each value in the list.

# COMMAND ----------

df_NewTagRows.display()

# COMMAND ----------

df_NewTagRows.write.format("delta").mode("overwrite").save("/FileStore/tables/df_NewTagRows")