# Databricks notebook source
# MAGIC %md
# MAGIC ### Challenge 7: Join Product and Sales Data
# MAGIC
# MAGIC Join two DataFrames on product_id to get product names with amounts.

# COMMAND ----------

from pyspark.sql import Row

products = [
    Row(product_id=1, product_name='Phone'),
    Row(product_id=2, product_name='Tablet')
]
sales = [
    Row(product_id=1, amount=500),
    Row(product_id=2, amount=800),
    Row(product_id=1, amount=200)
]
df_products = spark.createDataFrame(products)
df_sales = spark.createDataFrame(sales)
df_products.show()
df_sales.show()

# COMMAND ----------

df_Product_Sales = df_products.join(df_sales, "product_id", "inner")\
    .select("product_name", "amount")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### Using inner join to join two dataframes df_products and df_sales on "product_id". If we only want to show "product_name" and "amount" after joining use select otherwise just leave as it is to shows all columns.

# COMMAND ----------

df_Product_Sales.display()

# COMMAND ----------

df_Product_Sales.write.format("delta").mode("overwrite").save("/FileStore/tables/df_Product_Sales")