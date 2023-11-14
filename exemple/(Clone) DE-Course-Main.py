# Databricks notebook source
# MAGIC %md 
# MAGIC # Getting started

# COMMAND ----------

# MAGIC %md
# MAGIC ## Listing files

# COMMAND ----------

# MAGIC %fs ls dbfs:/

# COMMAND ----------

# MAGIC %fs ls dbfs:/Data/

# COMMAND ----------

# MAGIC %fs cat dbfs:/Data/create_db.sql

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMPORARY VIEW orders (
# MAGIC order_id INT,
# MAGIC order_date STRING,
# MAGIC order_customer_id INT,
# MAGIC order_status STRING
# MAGIC ) USING CSV
# MAGIC OPTIONS(
# MAGIC   path='dbfs:/Data/orders'
# MAGIC )

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM orders LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC # SPARK SQL EXEMPLES

# COMMAND ----------

# MAGIC %md
# MAGIC ##READING FROM TEXT USING SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM TEXT.`dbfs:/Data/orders`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders (
# MAGIC order_id INT,
# MAGIC order_date DATE,
# MAGIC order_customer_id INT,
# MAGIC order_status STRING
# MAGIC ) USING CSV
# MAGIC OPTIONS(
# MAGIC   path='dbfs:/Data/orders',
# MAGIC   separator=','
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM   TEXT.`dbfs:/Data/order_items`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW order_items(
# MAGIC   order_item_id INT,
# MAGIC   order_item_order_id INT,
# MAGIC   order_item_product_id INT,
# MAGIC   order_item_quantity INT ,
# MAGIC   order_item_subtotal FLOAT ,
# MAGIC   order_item_product_price FLOAT
# MAGIC ) USING CSV
# MAGIC OPTIONS(
# MAGIC   path='dbfs:/Data/order_items',
# MAGIC   separator=','
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM order_items LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving Join to parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE DIRECTORY 'dbfs://joined_orders'
# MAGIC USING PARQUET
# MAGIC SELECT * 
# MAGIC FROM orders as o
# MAGIC JOIN order_items as oi ON o.order_id=oi.order_item_order_id
# MAGIC WHERE o.order_status IN ('COMPLETE','CLOSED') 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE DIRECTORY 'file:/Workspace/Users/ismaelcoulibaly616@gmail.com/DE-COURSE/joined'
# MAGIC USING PARQUET
# MAGIC SELECT * 
# MAGIC FROM orders as o
# MAGIC JOIN order_items as oi ON o.order_id=oi.order_item_order_id
# MAGIC WHERE o.order_status IN ('COMPLETE','CLOSED') 

# COMMAND ----------

# MAGIC %fs ls 

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Pyspark

# COMMAND ----------

orders_df=spark.read.csv('dbfs:/Data/orders/',inferSchema=True).toDF('order_id','order_date','order_customer_id','order_status')

# COMMAND ----------

display(orders_df)

# COMMAND ----------

from pyspark.sql.functions import count,col

# COMMAND ----------

grouped_df=orders_df.groupBy('order_status').agg(count('*'))

# COMMAND ----------

grouped_df.show()

# COMMAND ----------


